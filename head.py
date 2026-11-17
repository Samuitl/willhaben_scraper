#!/usr/bin/env python3
"""
head.py – der "Kopf": ein Daemon, der Aufträge aus der Tabelle `enrichment_tasks` abarbeitet.

Auftragsarten (kind)
  detail   ruft die willhaben-Detail-API der Anzeige ab und speichert die Antwort unverändert
           als JSON in `ad_details` (Spalte data)
  images   lädt alle Bilder der Anzeige (Links aus `ad_images`) in das Medien-Verzeichnis und
           trägt sie in `ad_image_files` ein (Pfad, SHA-256, Größe)

Woher kommen Aufträge?
  1. Die Auswertungs-API (oder du per SQL) legt Zeilen in `enrichment_tasks` an:
       INSERT INTO enrichment_tasks (ad_id, kind, priority, created_at)
       VALUES (123456789, 'detail', 1, now()) ON CONFLICT DO NOTHING;
     Kleinere priority-Zahl = wird zuerst bearbeitet (Standard 5).
  2. Per Kommandozeile:  python head.py enqueue --kind images --ad 123 456
  3. Automatisch nach Regeln (config: head_auto_rules), z.B.
       [{"name": "Fahrräder über 1000 €", "marketplace_ids": [4552], "price_min": 1000,
         "kinds": ["detail", "images"], "limit": 100}]
     Pro Zyklus (head_auto_enqueue_minutes) werden höchstens `limit` neue Aufträge je Regel und Art
     angelegt, und zwar für aktive Anzeigen ohne bestehenden Auftrag, die neuesten zuerst.

Befehle
  python head.py              Daemon starten (Standard)
  python head.py status       Aufträge nach Art und Status zählen
  python head.py enqueue ...  Aufträge anlegen
  python head.py probe-detail 971448719   Detail-Antwort ansehen (zum Prüfen der API)

Fehlerbehandlung: Fehlgeschlagene Aufträge werden bis head_max_attempts mit wachsender Wartezeit
wiederholt (2, 4, 8 ... Minuten, höchstens 60). HTTP 404/410 = Anzeige/Bild existiert nicht mehr
-> Status 'gone', keine Wiederholung. Abgestürzte Aufträge ('running' länger als head_stale_minutes)
werden wieder eingereiht. Mehrere Kopf-Instanzen können parallel laufen (FOR UPDATE SKIP LOCKED).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import willhaben_scraper as w

log = logging.getLogger("willhaben.head")

KINDS = ("detail", "images")
CONTENT_EXT = {"image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
               "image/webp": ".webp", "image/gif": ".gif", "image/avif": ".avif"}


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


# --------------------------------------------------------------------------- #
# Warteschlange
# --------------------------------------------------------------------------- #
class TaskQueue:
    def __init__(self, db: w.Storage):
        self.db = db

    def enqueue(self, ad_ids: list[int], kind: str, priority: int = 5, force: bool = False) -> int:
        """Legt Aufträge an. Bestehende bleiben unverändert, außer force=True (dann neu einreihen)."""
        if kind not in KINDS:
            raise w.ScraperError(f"Unbekannte Auftragsart: {kind} (erlaubt: {', '.join(KINDS)})")
        now = w.utcnow()
        created = 0
        for ad_id in ad_ids:
            if force:
                cur = self.db._x(
                    """INSERT INTO enrichment_tasks (ad_id, kind, priority, status, created_at)
                       VALUES (?, ?, ?, 'pending', ?)
                       ON CONFLICT (ad_id, kind) DO UPDATE SET status = 'pending', attempts = 0,
                           error = NULL, next_try_at = NULL, priority = excluded.priority""",
                    (ad_id, kind, priority, now))
            else:
                cur = self.db._x(
                    """INSERT INTO enrichment_tasks (ad_id, kind, priority, status, created_at)
                       VALUES (?, ?, ?, 'pending', ?) ON CONFLICT (ad_id, kind) DO NOTHING""",
                    (ad_id, kind, priority, now))
            created += max(cur.rowcount, 0)
        self.db.conn.commit()
        return created

    def claim(self) -> Optional[dict]:
        """Holt den nächsten fälligen Auftrag und markiert ihn als 'running'."""
        now = w.utcnow()
        skip = " FOR UPDATE SKIP LOCKED" if self.db.pg else ""
        row = self.db._x(
            f"""UPDATE enrichment_tasks SET status = 'running', started_at = ?, attempts = attempts + 1
                WHERE id = (SELECT id FROM enrichment_tasks
                            WHERE status = 'pending' AND (next_try_at IS NULL OR next_try_at <= ?)
                            ORDER BY priority, id LIMIT 1{skip})
                RETURNING id, ad_id, kind, attempts""",
            (now, now)).fetchone()
        self.db.conn.commit()
        return dict(row) if row else None

    def finish(self, task_id: int, status: str, note: Optional[str] = None,
               retry_at: Optional[str] = None) -> None:
        self.db._x(
            """UPDATE enrichment_tasks SET status = ?, finished_at = ?, error = ?, next_try_at = ?
               WHERE id = ?""",
            (status, w.utcnow(), note, retry_at, task_id))
        self.db.conn.commit()

    def requeue_stale(self, minutes: int) -> int:
        cutoff = iso(datetime.now(timezone.utc) - timedelta(minutes=minutes))
        cur = self.db._x(
            "UPDATE enrichment_tasks SET status = 'pending' WHERE status = 'running' AND started_at < ?",
            (cutoff,))
        self.db.conn.commit()
        return max(cur.rowcount, 0)

    def stats(self) -> list[dict]:
        rows = self.db._x(
            "SELECT kind, status, COUNT(*) AS n FROM enrichment_tasks GROUP BY kind, status ORDER BY kind, status"
        ).fetchall()
        self.db.conn.commit()
        return [dict(r) for r in rows]

    def auto_enqueue(self, rules: list[dict]) -> int:
        """Legt Aufträge nach den Regeln aus der Config an (siehe Moduldokumentation)."""
        total = 0
        for rule in rules:
            limit = int(rule.get("limit", 100))
            kinds = rule.get("kinds") or ["detail"]
            for kind in kinds:
                if kind not in KINDS:
                    log.warning("Regel '%s': unbekannte Art %r übersprungen", rule.get("name"), kind)
                    continue
                where = ["a.is_active = 1",
                         "NOT EXISTS (SELECT 1 FROM enrichment_tasks t WHERE t.ad_id = a.id AND t.kind = ?)"]
                params: list[Any] = [kind]
                ids = rule.get("marketplace_ids")
                if ids:
                    where.append(f"a.marketplace_id IN ({','.join('?' * len(ids))})")
                    params += [int(i) for i in ids]
                if rule.get("price_min") is not None:
                    where.append("a.price >= ?")
                    params.append(float(rule["price_min"]))
                if rule.get("price_max") is not None:
                    where.append("a.price <= ?")
                    params.append(float(rule["price_max"]))
                # Explizite Casts: in INSERT ... SELECT kann PostgreSQL die Typen der Parameter nicht erraten
                ts = "TIMESTAMPTZ" if self.db.pg else "TEXT"
                sql = (f"""INSERT INTO enrichment_tasks (ad_id, kind, priority, status, created_at)
                           SELECT a.id, CAST(? AS TEXT), CAST(? AS INTEGER), 'pending', CAST(? AS {ts})
                           FROM ads a
                           WHERE {' AND '.join(where)}
                           ORDER BY a.first_seen_at DESC LIMIT ?
                           ON CONFLICT (ad_id, kind) DO NOTHING""")
                head = [kind, int(rule.get("priority", 5)), w.utcnow()]
                cur = self.db._x(sql, head + params + [limit])
                self.db.conn.commit()
                n = max(cur.rowcount, 0)
                if n:
                    log.info("Regel '%s': %d neue '%s'-Aufträge", rule.get("name", "?"), n, kind)
                total += n
        return total


# --------------------------------------------------------------------------- #
# Kopf
# --------------------------------------------------------------------------- #
class Head:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.db = w.open_storage(cfg)
        self.queue = TaskQueue(self.db)
        self.detail_client = w.HttpClient(dict(cfg, request_delay_seconds=cfg["head_detail_delay_seconds"]))
        self.image_client = w.HttpClient(dict(cfg, request_delay_seconds=cfg["head_image_delay_seconds"],
                                              session_init_url=None))
        self.media_dir = Path(cfg["head_media_dir"])

    def close(self) -> None:
        self.db.close()

    # ---- Abrufe ----------------------------------------------------------- #
    def fetch_detail(self, ad_id: int) -> str:
        url = self.cfg["head_detail_url_template"].format(ad_id=ad_id)
        data = self.detail_client.get_json(url)
        payload = json.dumps(data, ensure_ascii=False)
        self.db._x(
            """INSERT INTO ad_details (ad_id, fetched_at, data) VALUES (?, ?, ?)
               ON CONFLICT (ad_id) DO UPDATE SET fetched_at = excluded.fetched_at, data = excluded.data""",
            (ad_id, w.utcnow(), payload))
        self.db.conn.commit()
        return f"{len(payload)} Zeichen JSON"

    def fetch_images(self, ad_id: int) -> str:
        rows = self.db._x("SELECT position, url FROM ad_images WHERE ad_id = ? ORDER BY position",
                          (ad_id,)).fetchall()
        self.db.conn.commit()
        if not rows:
            return "keine Bilder vorhanden"
        saved = skipped = missing = 0
        for r in rows:
            if w.STOP_EVENT.is_set():
                raise KeyboardInterrupt
            position, url = int(r["position"]), r["url"]
            have = self.db._x("SELECT source_url, file_path FROM ad_image_files WHERE ad_id = ? AND position = ?",
                              (ad_id, position)).fetchone()
            self.db.conn.commit()
            if have and have["source_url"] == url and (self.media_dir / have["file_path"]).exists():
                skipped += 1
                continue
            try:
                resp = self.image_client.get(url, accept="image/avif,image/webp,image/*")
            except w.RequestFailed as e:
                if e.status in (404, 410):
                    missing += 1
                    log.info("Bild %s/%d existiert nicht mehr", ad_id, position)
                    continue
                raise
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            data = resp.content
            if not ctype.startswith("image/"):
                raise w.ScraperError(f"Kein Bild (Content-Type '{ctype}') unter {url}")
            if len(data) > self.cfg["head_image_max_bytes"]:
                raise w.ScraperError(f"Bild zu groß ({len(data)} Bytes) unter {url}")
            rel = f"{ad_id % 1000:03d}/{ad_id}/{position}{CONTENT_EXT.get(ctype, '.jpg')}"
            target = self.media_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(target.name + ".part")
            tmp.write_bytes(data)
            os.replace(tmp, target)
            self.db._x(
                """INSERT INTO ad_image_files (ad_id, position, source_url, file_path, sha256, bytes,
                                               content_type, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (ad_id, position) DO UPDATE SET source_url = excluded.source_url,
                       file_path = excluded.file_path, sha256 = excluded.sha256, bytes = excluded.bytes,
                       content_type = excluded.content_type, fetched_at = excluded.fetched_at""",
                (ad_id, position, url, rel, hashlib.sha256(data).hexdigest(), len(data), ctype, w.utcnow()))
            self.db.conn.commit()
            saved += 1
        return f"{saved} gespeichert, {skipped} vorhanden, {missing} nicht mehr verfügbar"

    # ---- Auftragsverarbeitung --------------------------------------------- #
    def process(self, task: dict) -> None:
        label = f"#{task['id']} {task['kind']} Anzeige {task['ad_id']} (Versuch {task['attempts']})"
        try:
            if task["kind"] == "detail":
                note = self.fetch_detail(task["ad_id"])
            elif task["kind"] == "images":
                note = self.fetch_images(task["ad_id"])
            else:
                raise w.ScraperError(f"Unbekannte Auftragsart: {task['kind']}")
            self.queue.finish(task["id"], "done", note)
            log.info("%s: fertig (%s)", label, note)
        except w.RequestFailed as e:
            if e.status in (404, 410):
                self.queue.finish(task["id"], "gone", str(e)[:300])
                log.info("%s: nicht mehr vorhanden (HTTP %s)", label, e.status)
            else:
                self._retry(task, label, e)
        except KeyboardInterrupt:
            # Beim Beenden: Auftrag sofort wieder freigeben, nicht als Fehlversuch zählen
            self.db.conn.rollback()
            self.db._x("UPDATE enrichment_tasks SET status = 'pending', attempts = attempts - 1 WHERE id = ?",
                       (task["id"],))
            self.db.conn.commit()
            raise
        except w.ScraperError as e:
            self._retry(task, label, e)
        except Exception as e:
            log.exception("%s: unerwarteter Fehler", label)
            self.db.conn.rollback()
            self._retry(task, label, e)

    def _retry(self, task: dict, label: str, error: Exception) -> None:
        note = f"{error.__class__.__name__}: {error}"[:400]
        if task["attempts"] >= self.cfg["head_max_attempts"]:
            self.queue.finish(task["id"], "failed", note)
            log.error("%s: endgültig fehlgeschlagen – %s", label, note)
            return
        wait_min = min(2 ** task["attempts"], 60)
        retry_at = iso(datetime.now(timezone.utc) + timedelta(minutes=wait_min))
        self.queue.finish(task["id"], "pending", note, retry_at)
        log.warning("%s: Fehler, neuer Versuch in %d min – %s", label, wait_min, note)

    # ---- Hauptschleife ---------------------------------------------------- #
    def run_forever(self) -> None:
        cfg = self.cfg
        self.db.ensure_api_grants(cfg.get("head_api_role"))
        n = self.queue.requeue_stale(cfg["head_stale_minutes"])
        if n:
            log.warning("%d abgestürzte Aufträge wieder eingereiht", n)
        log.info("Kopf läuft. Aufträge: %s", self.queue.stats() or "keine")
        last_housekeeping = 0.0
        errors = 0
        while not w.STOP_EVENT.is_set():
            w.touch_heartbeat(cfg)
            try:
                if time.monotonic() - last_housekeeping >= cfg["head_auto_enqueue_minutes"] * 60:
                    last_housekeeping = time.monotonic()
                    self.queue.requeue_stale(cfg["head_stale_minutes"])
                    if cfg.get("head_auto_rules"):
                        self.queue.auto_enqueue(cfg["head_auto_rules"])
                task = self.queue.claim()
                if task is None:
                    w.STOP_EVENT.wait(cfg["head_poll_seconds"])
                    continue
                self.process(task)
                errors = 0
            except KeyboardInterrupt:
                break
            except Exception:
                errors += 1
                log.exception("Fehler in der Hauptschleife (%d in Folge)", errors)
                if errors >= 5:
                    raise
                self._reconnect()
                w.STOP_EVENT.wait(10)
        log.info("Kopf beendet")

    def _reconnect(self) -> None:
        try:
            self.db.close()
        except Exception:                                  # pragma: no cover
            pass
        self.db = w.open_storage(self.cfg)
        self.queue = TaskQueue(self.db)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Kopf: Detail-Abrufe und Bilder")
    p.add_argument("--config", help="Pfad zur config.json")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command")
    sub.add_parser("run", help="Daemon starten (Standard)")
    sub.add_parser("status", help="Aufträge zählen")
    e = sub.add_parser("enqueue", help="Aufträge anlegen")
    e.add_argument("--kind", required=True, choices=list(KINDS) + ["both"])
    e.add_argument("--ad", type=int, nargs="*", default=[], help="Anzeigen-IDs")
    e.add_argument("--marketplace", type=int, help="oder: die neuesten aktiven Anzeigen dieses Marktplatzes")
    e.add_argument("--limit", type=int, default=100, help="Anzahl bei --marketplace (Standard 100)")
    e.add_argument("--priority", type=int, default=5)
    e.add_argument("--force", action="store_true", help="bestehende Aufträge erneut einreihen")
    d = sub.add_parser("probe-detail", help="Detail-Antwort einer Anzeige ansehen")
    d.add_argument("ad_id", type=int)
    return p


def cmd_enqueue(cfg: dict, args: argparse.Namespace) -> int:
    db = w.open_storage(cfg)
    try:
        ids = list(args.ad)
        if args.marketplace is not None:
            rows = db._x("""SELECT id FROM ads WHERE marketplace_id = ? AND is_active = 1
                            ORDER BY first_seen_at DESC LIMIT ?""", (args.marketplace, args.limit)).fetchall()
            db.conn.commit()
            ids += [r["id"] for r in rows]
        if not ids:
            raise w.ScraperError("Keine Anzeigen angegeben (--ad ... oder --marketplace ...)")
        known = db.known_ids(ids)
        unknown = sorted(set(ids) - known)
        if unknown:
            log.warning("%d IDs nicht in der Datenbank (übersprungen): %s", len(unknown), unknown[:10])
        kinds = list(KINDS) if args.kind == "both" else [args.kind]
        queue = TaskQueue(db)
        for kind in kinds:
            n = queue.enqueue(sorted(known), kind, args.priority, args.force)
            print(f"{kind}: {n} Aufträge angelegt ({len(known) - n} bestanden schon)")
        return 0
    finally:
        db.close()


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = w.load_config(args.config)
    w.normalize_paths(cfg)
    command = args.command or "run"
    w.setup_logging(cfg, verbose=args.verbose, write_file=(command == "run"))

    def on_signal(signum, _frame):
        log.warning("Signal %s empfangen – Kopf beendet sich nach dem aktuellen Auftrag", signum)
        w.STOP_EVENT.set()

    try:
        if command == "run":
            signal.signal(signal.SIGTERM, on_signal)
            signal.signal(signal.SIGINT, on_signal)
            head = Head(cfg)
            try:
                head.run_forever()
            finally:
                head.close()
            return 0
        if command == "status":
            db = w.open_storage(cfg)
            try:
                rows = TaskQueue(db).stats()
            finally:
                db.close()
            if not rows:
                print("Keine Aufträge.")
            for r in rows:
                print(f"{r['kind']:<8} {r['status']:<8} {r['n']:>8}")
            return 0
        if command == "enqueue":
            return cmd_enqueue(cfg, args)
        if command == "probe-detail":
            url = cfg["head_detail_url_template"].format(ad_id=args.ad_id)
            data = w.HttpClient(cfg).get_json(url)
            print(f"URL: {url}")
            print(f"Top-Level-Schlüssel: {list(data)}")
            print(json.dumps(data, ensure_ascii=False, indent=2)[:4000])
            return 0
    except w.ScraperError as e:
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
