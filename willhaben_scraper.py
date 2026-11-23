#!/usr/bin/env python3
"""
willhaben_scraper.py
====================

Teil 1  (selten nötig):  Kategorien holen
    python willhaben_scraper.py categories

Teil 2  (regelmäßig):    Anzeigen holen
    python willhaben_scraper.py scrape
    python willhaben_scraper.py scrape --kategorien 4552,6941
    python willhaben_scraper.py scrape --mode incremental

Hilfsbefehle
    python willhaben_scraper.py list                 # Marktplätze + IDs anzeigen
    python willhaben_scraper.py list -o marktplaetze.txt   # zusätzlich in Textdatei schreiben
    python willhaben_scraper.py probe 4552 --page 30 # eine Seite testen (Seitenlimit prüfen)

Einstellungen: config.json (siehe DEFAULT_CONFIG) und/oder Kommandozeile.
Priorität: Standardwerte < config.json < Kommandozeile.

Docker / PostgreSQL / Scheduler: siehe scheduler.py, Dockerfile und docker-compose.yml.
Datenbank-Einstellungen können per Umgebungsvariablen WH_<KEY> überschrieben werden,
z.B. WH_DB_BACKEND=postgres, WH_DB_HOST=db, WH_DB_PASSWORD_FILE=/run/secrets/db_app_password.

Scheduler-Betrieb
-----------------
* Relative Pfade (Datenbank, Log, Lock, debug) gelten relativ zum Ordner dieser Datei,
  egal von wo der Scheduler das Script startet.
* Läuft bereits ein 'categories'/'scrape'-Lauf, beendet sich ein zweiter Start sofort (Exit-Code 0).
* Exit-Code 1 = mindestens ein Marktplatz ist fehlgeschlagen, 130 = Strg+C.
* Log: willhaben.log (rotierend) + Terminal.
"""
from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import re
import sqlite3
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("willhaben")
API_MAX_PAGES = 999   # willhaben: Seite 1000 liefert HTTP 400 -> max. 999 * rows Anzeigen pro Abfrage
BASE_DIR = Path(__file__).resolve().parent
STOP_EVENT = threading.Event()   # wird vom Scheduler bei SIGTERM gesetzt -> Läufe beenden sich sauber

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #
DEFAULT_CONFIG: dict[str, Any] = {
    "db_path": "willhaben.db",
    "base_url": "https://www.willhaben.at",
    # Startseite für Teil 1 (Hauptkategorien)
    "root_url": "https://www.willhaben.at/kaufen-und-verkaufen",
    # {slug} wird durch z.B. "fahrraeder-4552" ersetzt
    "search_url_template": (
        "https://www.willhaben.at/webapi/ad-search/search/atz/seo/"
        "kaufen-und-verkaufen/marktplatz/{slug}"
    ),
    "image_base_url": "https://cache.willhaben.at/mmo/",
    "request_delay_seconds": 4.0,      # Grundpause zwischen ALLEN Anfragen eines Prozesses (Scraper-Threads zusammen)
    "rate_max_delay_seconds": 60.0,    # so weit darf sich die Pause nach 429-Sperren selbst erhöhen
    "rate_recover_after": 100,         # nach so vielen Anfragen ohne 429 sinkt die Pause um 20 % (bis zum Grundwert)
    "rate_state_file": "willhaben.rate.json",   # merkt sich die gelernte Pause über Neustarts hinweg
    "max_rate_limit_waits": 8,         # so viele 429-Sperren hintereinander pro Anfrage, dann gilt sie als fehlgeschlagen
    "rate_confirm_retries": 1,         # nach einer ersten 429 so oft erneut anfragen, bevor sie als Sperre gilt (0 = aus)
    "rate_confirm_delay_seconds": 5.0, # Pause vor dieser Bestätigungs-Anfrage
    "max_retry_after_seconds": 7200,   # längere Retry-After-Werte werden auf diesen Wert gekappt
    "rows_per_page": 200,              # Parameter rows
    "stop_below_items": 198,           # weniger Items auf einer Seite -> letzte Seite
    "max_pages": 999,                  # Seiten pro Abfrage; 999 ist das Limit der API (nicht höher setzen)
    "completeness_tolerance": 0.005,    # erlaubte Abweichung zu rowsFound (5 %)
    "incremental_min_new_per_page": 1, # incremental: stoppen, wenn weniger neue Anzeigen auf einer Seite
    "timeout_seconds": 30,
    "max_retries": 4,
    "retry_backoff_seconds": 5,
    "mode": "full",                    # full | incremental
    "categories": [],                  # IDs oder Namen (Haupt-, Unter-Kategorie oder Marktplatz); leer = alle
    "exclude_categories": [],
    "extra_params": {},                # zusätzliche Query-Parameter für die Suche
    "extra_headers": {},               # zusätzliche HTTP-Header (für alle Anfragen)
    # Header, die willhaben für die Such-API (webapi) erwartet; entspricht dem Browser
    "api_headers": {
        "x-wh-client": "api@willhaben.at;responsive_web;server;1.0.0;desktop",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    },
    # Seite, die vor der ersten API-Anfrage geladen wird, um Cookies (u.a. CSRF-Token) zu erhalten
    "session_init_url": "https://www.willhaben.at/iad/kaufen-und-verkaufen",
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "debug_dir": "debug",              # hier landen HTML-Seiten, die nicht geparst werden konnten
    "log_level": "INFO",
    "log_file": "willhaben.log",       # null/"" = keine Log-Datei
    "log_max_bytes": 5_000_000,
    "log_backups": 5,
    "lock_file": "willhaben.lock",     # verhindert gleichzeitige Läufe
    "max_consecutive_failures": 5,     # Abbruch nach so vielen Fehlern in Folge (0 = nie)
    "inactive_after_missed_runs": 2,   # so viele vollständige Full-Läufe ohne Anzeige -> inaktiv
    "oldest_first": True,              # am längsten nicht gescrapte Marktplätze zuerst
    "priority_categories": [],         # diese Kategorien (wie bei "categories") kommen in jedem Lauf zuerst
    "incremental_requires_full": True, # incremental überspringt Marktplätze ohne abgeschlossenen Full-Lauf
    # --- Datenbank: "sqlite" (Datei) oder "postgres" ---
    "db_backend": "sqlite",
    "db_host": "localhost",
    "db_port": 5432,
    "db_name": "willhaben",
    "db_user": "willhaben_app",
    "db_password_file": None,          # Pfad zur Passwort-Datei (Docker secret), z.B. /run/secrets/db_app_password
    # --- Proxy (ein einzelner Proxy für alle Anfragen an willhaben; mit false/true ein-/ausschalten) ---
    "proxy_enabled": False,            # true = Proxy benutzen, false = direkte Verbindung
    "proxy_url": None,                 # z.B. "http://proxy.example.com:8080" oder "socks5h://host:1080"
    "proxy_user": None,                # Benutzername (oder direkt in proxy_url: http://user:pass@host:port)
    "proxy_password": None,            # Passwort im Klartext (nicht empfohlen, besser proxy_password_file)
    "proxy_password_file": None,       # Pfad zur Passwort-Datei (Docker secret), z.B. /run/secrets/proxy_password
    "proxy_use_for": ["search", "detail", "images"],   # welche Anfragearten über den Proxy laufen
    # --- Scheduler (scheduler.py) ---
    "schedule_timezone": "Europe/Vienna",
    "schedule_incremental_minutes": 60,
    "schedule_full": {"weekday": "sun", "hour": 2, "minute": 0},
    "schedule_categories": {"day": 1, "hour": 1, "minute": 0},   # monatlich
    "full_on_start_if_incomplete": True,   # beim Start: fehlen Kategorien/Full-Läufe, sofort nachholen
    "heartbeat_file": None,            # wird regelmäßig berührt (Docker-Healthcheck), z.B. /tmp/scheduler.alive
    # --- Kopf (head.py): Detail-Abrufe und Bilder ---
    "head_poll_seconds": 5,
    "head_detail_url_template": "https://publicapi.willhaben.at/atdetail/v1/{ad_id}",
    "head_detail_delay_seconds": 1.0,
    "head_image_delay_seconds": 0.3,
    "head_media_dir": "media",
    "head_image_max_bytes": 15_000_000,
    "head_max_attempts": 5,
    "head_stale_minutes": 15,          # 'running'-Aufträge älter als das gelten als abgestürzt -> neu einreihen
    "head_auto_enqueue_minutes": 10,
    "head_auto_rules": [],             # automatisch Aufträge anlegen, siehe head.py / README
    "head_api_role": "willhaben_api",  # DB-Rolle der Auswertungs-API (darf Aufträge anlegen)
    # Marktplätze mit mehr als 999*rows Anzeigen werden (nur im Modus full) automatisch aufgeteilt.
    # Reihenfolge = Verschachtelung: erst Bundesland, ist ein Teil immer noch zu groß, dann Preis.
    # ACHTUNG: Parameternamen/Werte sind ungeprüft -> mit "splitcheck <ID>" kontrollieren!
    "split_enabled": True,
    "split_levels": [
        {"name": "Bundesland", "type": "values", "param": "areaId",
         "values": {"Burgenland": 1, "Kärnten": 2, "Niederösterreich": 3, "Oberösterreich": 4,
                    "Salzburg": 5, "Steiermark": 6, "Tirol": 7, "Vorarlberg": 8, "Wien": 900,
                    "Ausland": 22000}},
        {"name": "Preis", "type": "range", "param_from": "PRICE_FROM", "param_to": "PRICE_TO",
         # Obergrenze eines Teils = nächste Grenze - to_offset. 0.01 erfasst auch Cent-Preise (z.B. 4,50);
         # per probe bestätigt: die API akzeptiert Dezimalzahlen (4-4.99 > 4-4). Bei 1 gäbe es Lücken.
         "to_offset": 0.01,
         "edges": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                   15, 20,
                   30, 40, 50, 60, 70, 80, 90,
                   100, 110, 120, 130, 140, 150,
                   160, 170, 180, 190, 200,
                   210, 220, 230, 240, 250,
                   300, 350, 400, 450, 500,
                   550, 600, 650, 700, 750,
                   800, 850, 900, 950, 1000,
                   1100, 1200, 1300, 1400, 1500,
                   2000, 2500, 3000, 4000, 5000,
                   6000, 7000, 8000, 9000, 10000,
                   20000, 30000, 40000, 50000,
                   60000, 70000, 80000, 90000,
                   100000, 200000]},
    ],
}


def load_config(path: Optional[str]) -> dict[str, Any]:
    """Priorität: Standardwerte < config.json < Umgebungsvariablen WH_<KEY>."""
    cfg = dict(DEFAULT_CONFIG)
    p = Path(path) if path else BASE_DIR / "config.json"
    if p.exists():
        try:
            cfg.update(json.loads(p.read_text(encoding="utf-8")))
        except json.JSONDecodeError as e:
            raise SystemExit(f"Fehler in {p}: kein gültiges JSON ({e})")
    elif path:
        raise SystemExit(f"Konfigurationsdatei nicht gefunden: {p}")
    for key in DEFAULT_CONFIG:
        raw = os.environ.get("WH_" + key.upper())
        if raw is None:
            continue
        if raw == "":
            cfg[key] = None
            continue
        try:
            cfg[key] = json.loads(raw)       # Zahlen, true/false, Listen, JSON-Objekte
        except ValueError:
            cfg[key] = raw                   # sonst normaler Text
    return cfg


def resolve_path(value: str) -> str:
    """Relative Pfade beziehen sich auf den Ordner des Scripts, nicht auf das Arbeitsverzeichnis."""
    p = Path(value).expanduser()
    return str(p if p.is_absolute() else BASE_DIR / p)


class RunLock:
    """Datei-Lock, das das Betriebssystem bei jedem Programmende (auch Absturz) freigibt."""

    def __init__(self, path: str):
        self.path = Path(path)
        self._fh = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+")
        try:
            if os.name == "nt":
                import msvcrt
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if os.name == "nt":
                import msvcrt
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        finally:
            self._fh.close()
            self._fh = None


# --------------------------------------------------------------------------- #
# Fehler
# --------------------------------------------------------------------------- #
class ScraperError(Exception):
    """Basisklasse für erwartete Fehler."""


class RequestFailed(ScraperError):
    """HTTP-/Netzwerkfehler (nach allen Wiederholungen). `status` = HTTP-Statuscode, falls vorhanden."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class ParseError(ScraperError):
    """Antwort hatte nicht die erwartete Struktur."""


# --------------------------------------------------------------------------- #
# Hilfsfunktionen
# --------------------------------------------------------------------------- #
def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def ms_to_iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat(timespec="seconds")
    except (ValueError, OverflowError, OSError):
        return None


def to_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def to_int_count(text: str) -> Optional[int]:
    digits = re.sub(r"\D", "", text or "")
    return int(digits) if digits else None


_ADDR_RE = re.compile(
    r"^(?P<street>.+?)\s+"
    r"(?P<house>\d+[a-zA-Z]?(?:\s*[-–]\s*\d+[a-zA-Z]?)?)(?![\d.])"
    r"(?:[\s/,]+(?P<rest>.*))?$"
)


def parse_address(raw: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """'Vöslauer Straße 9' -> (Straße, Hausnummer, Tür). Fehlende Teile = None."""
    if not raw or not raw.strip():
        return None, None, None
    raw = raw.strip()
    m = _ADDR_RE.match(raw)
    if not m:
        return raw.strip(" ,"), None, None
    street = m.group("street").strip(" ,")
    house = re.sub(r"\s+", "", m.group("house"))
    door = (m.group("rest") or "").strip(" /,-") or None
    return street, house, door


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _block_info(resp: "requests.Response") -> str:
    """Kurzbeschreibung einer Sperr-Antwort (Server, Cloudflare-Kennung, sichtbarer Text) fürs Log."""
    h = resp.headers
    parts = [f"{k}={h[k]}" for k in ("Server", "CF-RAY", "cf-mitigated", "Retry-After") if k in h]
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["style", "script", "noscript"]):
            tag.decompose()
        text = " ".join(soup.get_text(" ", strip=True).split())[:250]
    except Exception:                                    # pragma: no cover
        text = ""
    return f"[{', '.join(parts)}] Text: {text or '(kein sichtbarer Text)'}"


class RateLimiter:
    """Gemeinsames, lernendes Tempo-Limit für alle Threads, die denselben Server ansprechen.

    * Höchstens eine Anfrage pro `delay` Sekunden - alle Threads zusammen (Zeitfenster-Reservierung).
    * HTTP 429: ALLE Threads pausieren bis zum Ende der Sperre (Retry-After). Danach wird `delay`
      verdoppelt (höchstens `max_delay`) - pro Sperre nur einmal, auch wenn mehrere Threads sie melden.
    * Nach `recover_after` Anfragen ohne 429 sinkt `delay` um 20 % (nie unter den Grundwert).
    * Die gelernte Pause wird in `state_file` gespeichert und beim Start wieder geladen.
    """

    def __init__(self, name: str, base_delay: float, max_delay: float = 60.0, recover_after: int = 200,
                 state_file: Optional[str] = None):
        self.name, self.base = name, max(0.0, float(base_delay))
        self.max_delay = max(self.base, float(max_delay))
        self.recover_after = max(1, int(recover_after))
        self.state_file = state_file
        self._lock = threading.Lock()
        self._next_slot = 0.0           # monotonic: frühester Start der nächsten Anfrage
        self._blocked_until = 0.0       # monotonic: bis dahin gilt die 429-Sperre
        self._ok_streak = 0
        self.delay = self.base
        saved = self._load()
        if saved and saved > self.base:
            self.delay = min(saved, self.max_delay)
            log.info("Tempo-Limit '%s': gelernte Pause %.1f s aus %s übernommen (Grundwert %.1f s)",
                     name, self.delay, state_file, self.base)

    def configure(self, base_delay: float, max_delay: float, recover_after: int) -> None:
        with self._lock:
            self.base = max(0.0, float(base_delay))
            self.max_delay = max(self.base, float(max_delay))
            self.recover_after = max(1, int(recover_after))
            self.delay = min(max(self.delay, self.base), self.max_delay)

    def acquire(self) -> float:
        """Reserviert den nächsten Anfrage-Zeitpunkt und gibt die Wartezeit in Sekunden zurück."""
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_slot, self._blocked_until)
            self._next_slot = start + self.delay
            return start - now

    def success(self) -> None:
        with self._lock:
            self._ok_streak += 1
            if self._ok_streak >= self.recover_after and self.delay > self.base:
                self._ok_streak = 0
                self.delay = max(self.base, self.delay * 0.8)
                log.info("Tempo-Limit '%s': %d Anfragen ohne 429 – Pause sinkt auf %.2f s",
                         self.name, self.recover_after, self.delay)
                self._save()

    def blocked(self, wait_seconds: float) -> bool:
        """Meldet eine 429-Sperre. Gibt True zurück, wenn es eine NEUE Sperre war (Pause wurde erhöht)."""
        with self._lock:
            now = time.monotonic()
            is_new = now >= self._blocked_until
            self._blocked_until = max(self._blocked_until, now + wait_seconds)
            self._ok_streak = 0
            if is_new:
                old = self.delay
                self.delay = min(self.max_delay, old * 2 if old > 0 else 1.0)
                self._save()
                if self.delay != old:
                    log.warning("Tempo-Limit '%s': Pause zwischen Anfragen %.1f s -> %.1f s", self.name, old, self.delay)
            return is_new

    def _load(self) -> Optional[float]:
        if not self.state_file:
            return None
        try:
            return float(json.loads(Path(self.state_file).read_text(encoding="utf-8"))["delay"])
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def _save(self) -> None:
        if not self.state_file:
            return
        try:
            tmp = Path(self.state_file + ".tmp")
            tmp.write_text(json.dumps({"delay": round(self.delay, 3), "saved": utcnow()}), encoding="utf-8")
            os.replace(tmp, self.state_file)
        except OSError as e:
            log.debug("Tempo-Status nicht speicherbar: %s", e)


_LIMITERS: dict[str, RateLimiter] = {}
_LIMITERS_LOCK = threading.Lock()


def get_limiter(key: str, cfg: dict) -> RateLimiter:
    """Ein Limiter pro Schlüssel und Prozess: alle HttpClient mit demselben Schlüssel teilen sich das Tempo."""
    with _LIMITERS_LOCK:
        lim = _LIMITERS.get(key)
        base = float(cfg["request_delay_seconds"])
        mx = float(cfg.get("rate_max_delay_seconds", 60.0))
        rec = int(cfg.get("rate_recover_after", 100))
        if lim is None:
            state = cfg.get("rate_state_file") if key == "search" else None
            lim = _LIMITERS[key] = RateLimiter(key, base, mx, rec, state)
        else:
            lim.configure(base, mx, rec)
        return lim


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Retry-After als Sekundenzahl oder HTTP-Datum -> Sekunden (None, wenn nicht lesbar)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(value)
        return max(0.0, (dt - datetime.now(dt.tzinfo or timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None


PROXY_SCHEMES = ("http", "https", "socks4", "socks4a", "socks5", "socks5h")


def read_proxy_password(cfg: dict) -> Optional[str]:
    pf = cfg.get("proxy_password_file")
    if pf:
        try:
            return Path(pf).read_text(encoding="utf-8").strip()
        except OSError as e:
            raise ScraperError(f"Proxy-Passwort-Datei nicht lesbar ({pf}): {e}") from e
    return cfg.get("proxy_password") or None


def proxy_url_for(cfg: dict[str, Any], kind: str = "search") -> Optional[str]:
    """Vollständige Proxy-URL (mit Benutzer/Passwort) oder None, wenn für `kind` kein Proxy gilt."""
    if not cfg.get("proxy_enabled"):
        return None
    if kind not in (cfg.get("proxy_use_for") or []):
        return None
    raw = str(cfg.get("proxy_url") or "").strip()
    if not raw:
        raise ScraperError("proxy_enabled ist true, aber proxy_url ist leer.")
    if "://" not in raw:
        raw = "http://" + raw
    u = urlsplit(raw)
    if u.scheme.lower() not in PROXY_SCHEMES:
        raise ScraperError(f"Proxy-Schema '{u.scheme}' wird nicht unterstützt (erlaubt: {', '.join(PROXY_SCHEMES)}).")
    if not u.hostname:
        raise ScraperError(f"proxy_url ist ungültig: {redact_proxy(raw)}")
    try:
        port = u.port
    except ValueError as e:
        raise ScraperError(f"proxy_url hat einen ungültigen Port: {redact_proxy(raw)}") from e
    if u.username is not None:                        # Zugangsdaten stehen schon in der URL
        return raw
    user = cfg.get("proxy_user")
    password = read_proxy_password(cfg)
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
    netloc = host + (f":{port}" if port else "")
    if user:
        cred = quote(str(user), safe="")
        if password:
            cred += ":" + quote(password, safe="")
        netloc = cred + "@" + netloc
    return urlunsplit((u.scheme, netloc, u.path, u.query, u.fragment))


def redact_proxy(url: str) -> str:
    """Proxy-URL ohne Passwort für Logs."""
    try:
        u = urlsplit(url)
        if u.username is None:
            return url
        host = f"[{u.hostname}]" if u.hostname and ":" in u.hostname else (u.hostname or "")
        netloc = f"{u.username}:***@{host}" + (f":{u.port}" if u.port else "")
        return urlunsplit((u.scheme, netloc, u.path, u.query, u.fragment))
    except ValueError:
        return "<ungültige Proxy-URL>"


_PROXY_LOGGED: set[str] = set()


class HttpClient:
    def __init__(self, cfg: dict[str, Any], limiter: str = "search"):
        self.cfg = cfg
        self.limiter = get_limiter(limiter, cfg)
        self.session = requests.Session()
        self.proxy = proxy_url_for(cfg, limiter)
        if self.proxy:
            self.session.trust_env = False            # nur dieser Proxy, keine Umgebungsvariablen
            self.session.proxies = {"http": self.proxy, "https": self.proxy}
        if limiter not in _PROXY_LOGGED:
            _PROXY_LOGGED.add(limiter)
            log.info("Verbindung '%s': %s", limiter,
                     f"über Proxy {redact_proxy(self.proxy)}" if self.proxy else "direkt (ohne Proxy)")
        self.session.headers.update({
            "User-Agent": cfg["user_agent"],
            "Accept-Language": "de-AT,de;q=0.9,en;q=0.5",
            **cfg.get("extra_headers", {}),
        })
        self._session_ready = False

    def _scrub(self, text: str) -> str:
        """Entfernt Proxy-Zugangsdaten aus Fehlertexten."""
        if self.proxy:
            u = urlsplit(self.proxy)
            for secret in {self.proxy, u.password or "", quote(u.password or "", safe="")}:
                if secret:
                    text = text.replace(secret, redact_proxy(self.proxy) if secret == self.proxy else "***")
        return text

    @staticmethod
    def _wait(seconds: float) -> None:
        """Wartet unterbrechbar: wird STOP_EVENT gesetzt (SIGTERM), endet die Wartezeit sofort."""
        if STOP_EVENT.wait(max(0.0, seconds)):
            raise KeyboardInterrupt

    def get(self, url: str, params: Optional[dict] = None, accept: str = "text/html",
            headers: Optional[dict] = None) -> requests.Response:
        cfg = self.cfg
        retries, max_blocks = cfg["max_retries"], cfg.get("max_rate_limit_waits", 8)
        max_after = float(cfg.get("max_retry_after_seconds", 7200))
        req_headers = {"Accept": accept, **(headers or {})}
        attempt = 0        # Netzwerk-/Serverfehler dieser Anfrage
        blocks = 0         # 429-Sperren dieser Anfrage (zählen NICHT als Fehlversuch)
        confirmed = 0      # Bestätigungs-Anfragen nach einer ersten 429
        while True:
            self._wait(self.limiter.acquire())          # gemeinsames Tempo, wartet auch eine 429-Sperre ab
            retry_after: Optional[float] = None
            try:
                resp = self.session.get(url, params=params, headers=req_headers, timeout=cfg["timeout_seconds"])
            except (requests.Timeout, requests.ConnectionError,
                    requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError) as e:
                problem = self._scrub(f"Netzwerkfehler: {e.__class__.__name__}: {str(e)[:200]}")
                if self.proxy and isinstance(e, requests.exceptions.ProxyError):
                    problem += f" [Proxy {redact_proxy(self.proxy)}]"
            else:
                code = resp.status_code
                if code == 200:
                    self.limiter.success()
                    if confirmed:
                        log.info("Bestätigungs-Anfrage erfolgreich – die 429 war einmalig, keine Sperre")
                    return resp
                if code == 407:
                    raise RequestFailed(
                        f"HTTP 407 – der Proxy ({redact_proxy(self.proxy or '-')}) verlangt Zugangsdaten oder lehnt "
                        f"sie ab. proxy_user/proxy_password(_file) prüfen.", status=407)
                if code in (401, 403):
                    raise RequestFailed(
                        f"HTTP {code} für {resp.url} – Zugriff verweigert/blockiert. {_block_info(resp)}",
                        status=code,
                    )
                if code == 429 and blocks == 0 and confirmed < int(cfg.get("rate_confirm_retries", 1)):
                    confirmed += 1
                    pause = float(cfg.get("rate_confirm_delay_seconds", 5.0))
                    log.warning("HTTP 429 (Retry-After %s) – eine Bestätigungs-Anfrage in %.0f s; erst eine zweite 429 "
                                "gilt als Sperre", resp.headers.get("Retry-After", "-"), pause)
                    self._wait(pause)
                    continue
                if code == 429:
                    blocks += 1
                    ra = parse_retry_after(resp.headers.get("Retry-After"))
                    wait = min(ra if ra is not None else 300.0 * blocks, max_after)
                    if blocks > max_blocks:
                        raise RequestFailed(f"HTTP 429 – {blocks} Sperren hintereinander bei {resp.url}", status=429)
                    self.limiter.blocked(wait)
                    log.warning("HTTP 429 (Sperre %d/%d) – alle Anfragen pausieren %.0f s (bis ca. %s)",
                                blocks, max_blocks, wait,
                                (datetime.now() + timedelta(seconds=wait)).strftime("%H:%M:%S"))
                    continue
                if code in (500, 502, 503, 504):
                    problem = f"HTTP {code}"
                    retry_after = parse_retry_after(resp.headers.get("Retry-After"))
                else:
                    body = " ".join(resp.text.split())[:300]
                    raise RequestFailed(f"HTTP {code} für {resp.url} | Antwort: {body or '(leer)'}", status=code)

            attempt += 1
            if attempt > retries:
                raise RequestFailed(f"{problem} – aufgegeben nach {attempt} Versuchen: {url}")
            wait = min(retry_after or cfg["retry_backoff_seconds"] * 2 ** (attempt - 1), max_after)
            log.warning("%s (Versuch %d/%d) – neuer Versuch in %.0f s", problem, attempt, retries + 1, wait)
            self._wait(wait)

    def get_text(self, url: str) -> str:
        resp = self.get(url)
        resp.encoding = "utf-8"
        return resp.text

    def _init_session(self) -> None:
        """Einmalig die Webseite laden, damit die Cookies (inkl. CSRF-Token) gesetzt werden."""
        if self._session_ready:
            return
        self._session_ready = True
        init_url = self.cfg.get("session_init_url")
        if not init_url:
            return
        try:
            self.get(init_url)
            log.debug("Session initialisiert, Cookies: %s", list(self.session.cookies.keys()))
        except RequestFailed as e:
            log.warning("Session-Initialisierung fehlgeschlagen (%s) – versuche es trotzdem.", e)

    def get_json(self, url: str, params: Optional[dict] = None, referer: Optional[str] = None) -> dict:
        self._init_session()
        headers = dict(self.cfg.get("api_headers", {}))
        if referer:
            headers["Referer"] = referer
        csrf = self.session.cookies.get("x-bbx-csrf-token")
        if csrf:
            headers["x-bbx-csrf-token"] = csrf
        resp = self.get(url, params=params, accept="application/json", headers=headers)
        try:
            data = resp.json()
        except ValueError as e:
            raise ParseError(f"Antwort ist kein JSON ({resp.url}): {e}") from e
        if not isinstance(data, dict):
            raise ParseError(f"Unerwartetes JSON-Format ({resp.url})")
        return data


# --------------------------------------------------------------------------- #
# Datenbank (alles SQL steht in dieser Klasse -> einfacher DB-Wechsel)
# --------------------------------------------------------------------------- #
LOOKUP_TABLES = {          # Tabelle -> Spaltenname
    "streets": "name",
    "house_numbers": "number",
    "door_numbers": "door",
    "cities": "name",
    "postcodes": "code",
    "states": "name",
    "districts": "name",
}

# Dialektabhängig sind nur: die Spaltentypen (build_schema), die Platzhalter (_q), die Zeilensperre
# FOR UPDATE und der Verbindungsaufbau. Alles andere ist Standard-SQL für SQLite und PostgreSQL.
_SCHEMA_TEMPLATE = """
CREATE TABLE IF NOT EXISTS main_categories (
    id {INT_PK},
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    url TEXT,
    updated_at {TS} NOT NULL
);
CREATE TABLE IF NOT EXISTS sub_categories (
    id {INT_PK},
    main_category_id {INT} NOT NULL REFERENCES main_categories(id),
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    url TEXT,
    ad_count {INT},
    updated_at {TS} NOT NULL
);
CREATE TABLE IF NOT EXISTS marketplaces (
    id {INT_PK},
    main_category_id {INT} NOT NULL REFERENCES main_categories(id),
    sub_category_id {INT} NOT NULL REFERENCES sub_categories(id),
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    url TEXT,
    ad_count {INT},
    updated_at {TS} NOT NULL,
    last_scraped_at {TS},
    last_full_scrape_at {TS}
);

CREATE TABLE IF NOT EXISTS streets       (id {AUTO_PK}, name   TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS house_numbers (id {AUTO_PK}, number TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS door_numbers  (id {AUTO_PK}, door   TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS cities        (id {AUTO_PK}, name   TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS postcodes     (id {AUTO_PK}, code   TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS states        (id {AUTO_PK}, name   TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS districts     (id {AUTO_PK}, name   TEXT NOT NULL UNIQUE);

CREATE TABLE IF NOT EXISTS ads (
    id {INT_PK},
    uuid TEXT,
    marketplace_id {INT} REFERENCES marketplaces(id),
    category_tree_ids TEXT,
    url TEXT NOT NULL,
    title TEXT,
    description TEXT,
    price {REAL},
    price_display TEXT,
    street_id {INT} REFERENCES streets(id),
    house_number_id {INT} REFERENCES house_numbers(id),
    door_number_id {INT} REFERENCES door_numbers(id),
    postcode_id {INT} REFERENCES postcodes(id),
    city_id {INT} REFERENCES cities(id),
    district_id {INT} REFERENCES districts(id),
    state_id {INT} REFERENCES states(id),
    address_raw TEXT,
    latitude {REAL},
    longitude {REAL},
    is_private {INT},
    advertiser_org_id TEXT,
    status TEXT,
    published_at {TS},
    changed_at {TS},
    end_date {TS},
    first_seen_at {TS} NOT NULL,
    last_seen_at {TS} NOT NULL,
    is_active {INT} NOT NULL DEFAULT 1,
    missed_runs {INT} NOT NULL DEFAULT 0,
    inactive_since {TS}
);
CREATE INDEX IF NOT EXISTS idx_ads_marketplace ON ads (marketplace_id, is_active);
CREATE INDEX IF NOT EXISTS idx_ads_published   ON ads (published_at);

CREATE TABLE IF NOT EXISTS ad_images (
    id {AUTO_PK},
    ad_id {INT} NOT NULL REFERENCES ads(id),
    position {INT} NOT NULL,
    url TEXT NOT NULL,
    UNIQUE (ad_id, position)
);

CREATE TABLE IF NOT EXISTS price_history (
    id {AUTO_PK},
    ad_id {INT} NOT NULL REFERENCES ads(id),
    old_price {REAL},
    new_price {REAL},
    recorded_at {TS} NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_price_history_ad ON price_history (ad_id);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id {AUTO_PK},
    marketplace_id {INT} REFERENCES marketplaces(id),
    mode TEXT,
    started_at {TS} NOT NULL,
    finished_at {TS},
    pages {INT} DEFAULT 0,
    ads_seen {INT} DEFAULT 0,
    ads_new {INT} DEFAULT 0,
    ads_updated {INT} DEFAULT 0,
    price_changes {INT} DEFAULT 0,
    ads_marked_inactive {INT} DEFAULT 0,
    rows_found {INT},
    complete {INT},
    status TEXT,
    stop_reason TEXT
);

CREATE TABLE IF NOT EXISTS enrichment_tasks (
    id {AUTO_PK},
    ad_id {INT} NOT NULL REFERENCES ads(id),
    kind TEXT NOT NULL,
    priority {INT} NOT NULL DEFAULT 5,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts {INT} NOT NULL DEFAULT 0,
    next_try_at {TS},
    started_at {TS},
    finished_at {TS},
    error TEXT,
    created_at {TS} NOT NULL,
    UNIQUE (ad_id, kind)
);
CREATE INDEX IF NOT EXISTS idx_tasks_pending ON enrichment_tasks (status, priority, id);

CREATE TABLE IF NOT EXISTS ad_details (
    ad_id {INT_PK} REFERENCES ads(id),
    fetched_at {TS} NOT NULL,
    data {JSON} NOT NULL
);

CREATE TABLE IF NOT EXISTS ad_image_files (
    id {AUTO_PK},
    ad_id {INT} NOT NULL REFERENCES ads(id),
    position {INT} NOT NULL,
    source_url TEXT NOT NULL,
    file_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    bytes {INT} NOT NULL,
    content_type TEXT,
    fetched_at {TS} NOT NULL,
    UNIQUE (ad_id, position)
)
"""


def build_schema(dialect: str) -> list[str]:
    """Liefert die CREATE-Statements für 'sqlite' oder 'postgres'."""
    pg = dialect == "postgres"
    types = {
        "{INT_PK}": "BIGINT PRIMARY KEY" if pg else "INTEGER PRIMARY KEY",
        "{AUTO_PK}": ("BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY" if pg
                      else "INTEGER PRIMARY KEY AUTOINCREMENT"),
        "{INT}": "BIGINT" if pg else "INTEGER",
        "{REAL}": "DOUBLE PRECISION" if pg else "REAL",
        "{TS}": "TIMESTAMPTZ" if pg else "TEXT",
        "{JSON}": "JSONB" if pg else "TEXT",
    }
    sql = _SCHEMA_TEMPLATE
    for key, value in types.items():
        sql = sql.replace(key, value)
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


_NAMED_PARAM = re.compile(r"(?<![:\w]):(\w+)")


class Storage:
    """Datenzugriff für SQLite (Datei) und PostgreSQL.

    Nebenläufigkeit (mehrere Läufe gleichzeitig, v.a. PostgreSQL):
      * Lookup-Einträge (Straße, Ort, ...) per INSERT ... ON CONFLICT DO NOTHING auf einer eigenen,
        sofort bestätigenden Verbindung -> kein Konflikt, kein Warten auf fremde Transaktionen.
      * Neue Anzeigen: INSERT ... ON CONFLICT DO NOTHING; bestehende Zeilen werden mit FOR UPDATE
        gesperrt -> Preisänderungen werden nicht doppelt protokolliert.
      * Ein älterer Stand (changed_at kleiner als gespeichert) überschreibt keine Inhalte.
      * Anzeigen werden pro Seite nach ID sortiert geschrieben; bei Deadlock/Serialisierungsfehler
        wird die Seite wiederholt.
    """

    def __init__(self, path: Optional[str] = None, dialect: str = "sqlite", pg: Optional[dict] = None):
        self.dialect = dialect
        self.pg = dialect == "postgres"
        self._cache: dict[tuple[str, str], int] = {}
        if self.pg:
            self.conn = self._connect_pg(pg or {}, autocommit=False)
            self.lconn = self._connect_pg(pg or {}, autocommit=True)
            self._x("SELECT pg_advisory_xact_lock(?)", (727274,))   # Schema-Erstellung nicht parallel
        else:
            self.conn = sqlite3.connect(path, timeout=30)
            self.conn.row_factory = sqlite3.Row
            self.lconn = self.conn
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA busy_timeout = 30000")
            self.conn.execute("PRAGMA journal_mode = WAL")   # Lesen (z.B. DB Browser) während des Laufs
            self.conn.execute("PRAGMA synchronous = NORMAL")
        for stmt in build_schema(dialect):
            self._x(stmt)
        if not self.pg:
            self._migrate()
        self.conn.commit()

    # ---- Verbindung / SQL-Hilfen ------------------------------------------ #
    @staticmethod
    def _connect_pg(pg: dict, autocommit: bool):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as e:
            raise ScraperError('Für PostgreSQL wird psycopg benötigt: pip install "psycopg[binary]"') from e
        last: Optional[Exception] = None
        for attempt in range(1, 11):
            try:
                conn = psycopg.connect(row_factory=dict_row, autocommit=autocommit, connect_timeout=10, **pg)
                conn.execute("SET TIME ZONE 'UTC'")
                if not autocommit:
                    conn.commit()
                return conn
            except psycopg.OperationalError as e:
                last = e
                log.warning("PostgreSQL nicht erreichbar (Versuch %d/10): %s", attempt, str(e).strip()[:150])
                time.sleep(3)
        raise ScraperError(f"PostgreSQL-Verbindung fehlgeschlagen: {last}")

    def _q(self, sql: str) -> str:
        """Platzhalter: '?' und ':name' (SQLite-Stil) -> '%s' und '%(name)s' für psycopg."""
        if not self.pg:
            return sql
        return _NAMED_PARAM.sub(r"%(\1)s", sql).replace("?", "%s")

    def _x(self, sql: str, params: Any = (), conn: Any = None):
        return (conn or self.conn).execute(self._q(sql), params)

    def _xmany(self, sql: str, seq: list) -> None:
        cur = self.conn.cursor()
        cur.executemany(self._q(sql), seq)

    def _migrate(self) -> None:
        """Nur SQLite: ergänzt Spalten in bereits bestehenden Datenbanken."""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(ads)")}
        if "missed_runs" not in cols:
            log.info("Datenbank-Update: Spalte ads.missed_runs wird ergänzt")
            self.conn.execute("ALTER TABLE ads ADD COLUMN missed_runs INTEGER NOT NULL DEFAULT 0")

    def close(self) -> None:
        for c in {id(self.conn): self.conn, id(self.lconn): self.lconn}.values():
            try:
                c.close()
            except Exception:                      # pragma: no cover
                pass

    @staticmethod
    def _iso(value: Any) -> Optional[str]:
        """Zeitstempel (str oder datetime) in ein einheitliches UTC-ISO-Format bringen."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat(timespec="seconds")
        return str(value)

    @staticmethod
    def _retryable(e: Exception) -> bool:
        if getattr(e, "sqlstate", None) in ("40P01", "40001"):      # deadlock / serialization failure
            return True
        return isinstance(e, sqlite3.OperationalError) and "locked" in str(e).lower()

    # ---- Kategorien ------------------------------------------------------- #
    def upsert_main(self, c: "CategoryLink", url: str) -> None:
        self._x(
            """INSERT INTO main_categories (id, name, slug, url, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET name=excluded.name, slug=excluded.slug,
                   url=excluded.url, updated_at=excluded.updated_at""",
            (c.id, c.name, c.slug, url, utcnow()),
        )

    def upsert_sub(self, c: "CategoryLink", main_id: int, url: str) -> None:
        self._x(
            """INSERT INTO sub_categories (id, main_category_id, name, slug, url, ad_count, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET main_category_id=excluded.main_category_id,
                   name=excluded.name, slug=excluded.slug, url=excluded.url,
                   ad_count=excluded.ad_count, updated_at=excluded.updated_at""",
            (c.id, main_id, c.name, c.slug, url, c.count, utcnow()),
        )

    def upsert_marketplace(self, c: "CategoryLink", main_id: int, sub_id: int, url: str) -> None:
        self._x(
            """INSERT INTO marketplaces (id, main_category_id, sub_category_id, name, slug, url,
                                         ad_count, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET main_category_id=excluded.main_category_id,
                   sub_category_id=excluded.sub_category_id, name=excluded.name,
                   slug=excluded.slug, url=excluded.url, ad_count=excluded.ad_count,
                   updated_at=excluded.updated_at""",
            (c.id, main_id, sub_id, c.name, c.slug, url, c.count, utcnow()),
        )

    def main_categories(self) -> list[dict]:
        return [dict(r) for r in self._x("SELECT * FROM main_categories ORDER BY id")]

    def sub_categories(self) -> list[dict]:
        return [dict(r) for r in self._x("SELECT * FROM sub_categories ORDER BY id")]

    def marketplaces(self) -> list[dict]:
        rows = self._x(
            """SELECT m.*, s.name AS sub_name, c.name AS main_name,
                      (SELECT COUNT(*) FROM ads a WHERE a.marketplace_id = m.id AND a.is_active = 1)
                          AS db_active_ads
               FROM marketplaces m
               JOIN sub_categories s ON s.id = m.sub_category_id
               JOIN main_categories c ON c.id = m.main_category_id
               ORDER BY c.name, s.name, m.name"""
        )
        result = [dict(r) for r in rows]
        self.conn.commit()
        return result

    def marketplace_counts(self) -> tuple[int, int]:
        """(Anzahl Marktplätze, davon ohne abgeschlossenen Full-Lauf)"""
        row = self._x(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN last_full_scrape_at IS NULL THEN 1 ELSE 0 END) AS never_full
               FROM marketplaces"""
        ).fetchone()
        self.conn.commit()
        return int(row["total"] or 0), int(row["never_full"] or 0)

    # ---- Lookup-Tabellen -------------------------------------------------- #
    def lookup_id(self, table: str, value: Optional[str]) -> Optional[int]:
        if value is None or str(value).strip() == "":
            return None
        value = str(value).strip()
        key = (table, value)
        if key in self._cache:
            return self._cache[key]
        col = LOOKUP_TABLES[table]
        self._x(f"INSERT INTO {table} ({col}) VALUES (?) ON CONFLICT ({col}) DO NOTHING",
                (value,), self.lconn)
        row = self._x(f"SELECT id FROM {table} WHERE {col} = ?", (value,), self.lconn).fetchone()
        rid = row["id"]
        self._cache[key] = rid
        return rid

    # ---- Anzeigen --------------------------------------------------------- #
    def known_ids(self, ids: list[int]) -> set[int]:
        if not ids:
            return set()
        marks = ",".join("?" * len(ids))
        rows = self._x(f"SELECT id FROM ads WHERE id IN ({marks})", ids).fetchall()
        self.conn.commit()
        return {r["id"] for r in rows}

    def save_ads(self, ads: list[dict], marketplace_id: int, now: str) -> Counter:
        ads = sorted(ads, key=lambda a: a["id"])          # feste Reihenfolge -> weniger Deadlocks
        for attempt in range(1, 4):
            stats: Counter = Counter()
            try:
                for ad in ads:
                    status, price_changed = self._upsert_ad(ad, marketplace_id, now)
                    stats[status] += 1
                    stats["price_changes"] += int(price_changed)
                self.conn.commit()
                return stats
            except Exception as e:
                self.conn.rollback()
                if not self.pg:
                    self._cache.clear()    # SQLite: Lookups liegen in derselben Transaktion
                if self._retryable(e) and attempt < 3:
                    log.warning("Datenbank-Konflikt (%s) – Seite wird wiederholt (%d/3)",
                                e.__class__.__name__, attempt)
                    time.sleep(0.5 * attempt)
                    continue
                raise
        raise ScraperError("save_ads: unerreichbar")      # pragma: no cover

    def _upsert_ad(self, ad: dict, marketplace_id: int, now: str) -> tuple[str, bool]:
        L = self.lookup_id
        values = {
            "id": ad["id"],
            "uuid": ad["uuid"],
            "marketplace_id": marketplace_id,
            "category_tree_ids": ad["category_tree_ids"],
            "url": ad["url"],
            "title": ad["title"],
            "description": ad["description"],
            "price": ad["price"],
            "price_display": ad["price_display"],
            "street_id": L("streets", ad["street"]),
            "house_number_id": L("house_numbers", ad["house_number"]),
            "door_number_id": L("door_numbers", ad["door"]),
            "postcode_id": L("postcodes", ad["postcode"]),
            "city_id": L("cities", ad["city"]),
            "district_id": L("districts", ad["district"]),
            "state_id": L("states", ad["state"]),
            "address_raw": ad["address_raw"],
            "latitude": ad["latitude"],
            "longitude": ad["longitude"],
            "is_private": ad["is_private"],
            "advertiser_org_id": ad["org_id"],
            "status": ad["status"],
            "published_at": ad["published_at"],
            "changed_at": ad["changed_at"],
            "end_date": ad["end_date"],
            "last_seen_at": now,
            "is_active": 1,
            "missed_runs": 0,
            "inactive_since": None,
        }
        select_old = ("SELECT price, title, description, changed_at FROM ads WHERE id = ?"
                      + (" FOR UPDATE" if self.pg else ""))
        old = self._x(select_old, (ad["id"],)).fetchone()

        if old is None:
            ins = {**values, "first_seen_at": now}
            cols = ", ".join(ins)
            marks = ", ".join(f":{k}" for k in ins)
            cur = self._x(f"INSERT INTO ads ({cols}) VALUES ({marks}) ON CONFLICT (id) DO NOTHING", ins)
            if cur.rowcount == 1:
                if ad["price"] is not None:
                    self._price_history(ad["id"], None, ad["price"], now)
                self._sync_images(ad["id"], ad["images"])
                return "new", False
            # ein paralleler Lauf war schneller -> wie eine bestehende Anzeige behandeln
            old = self._x(select_old, (ad["id"],)).fetchone()
            if old is None:                                    # pragma: no cover
                raise ScraperError(f"Anzeige {ad['id']} weder anlegbar noch lesbar")

        old_changed = self._iso(old["changed_at"])
        if ad["changed_at"] and old_changed and ad["changed_at"] < old_changed:
            # Älterer Stand (z.B. langsamer Full-Lauf nach aktuellerem Incremental-Lauf):
            # Inhalte nicht überschreiben, nur als gesehen markieren.
            self._x(
                """UPDATE ads SET last_seen_at = ?, is_active = 1, missed_runs = 0, inactive_since = NULL
                   WHERE id = ?""", (now, ad["id"]))
            return "unchanged", False

        price_changed = old["price"] != ad["price"]
        content_changed = (
            price_changed
            or old["title"] != ad["title"]
            or old["description"] != ad["description"]
            or old_changed != ad["changed_at"]
        )
        sets = ", ".join(f"{k} = :{k}" for k in values if k != "id")
        self._x(f"UPDATE ads SET {sets} WHERE id = :id", values)
        if price_changed:
            self._price_history(ad["id"], old["price"], ad["price"], now)
        self._sync_images(ad["id"], ad["images"])
        return ("updated" if content_changed else "unchanged"), price_changed

    def _price_history(self, ad_id: int, old: Optional[float], new: Optional[float], now: str) -> None:
        self._x(
            "INSERT INTO price_history (ad_id, old_price, new_price, recorded_at) VALUES (?, ?, ?, ?)",
            (ad_id, old, new, now),
        )

    def _sync_images(self, ad_id: int, urls: list[str]) -> None:
        current = [r["url"] for r in self._x(
            "SELECT url FROM ad_images WHERE ad_id = ? ORDER BY position", (ad_id,)).fetchall()]
        if current == urls:
            return
        self._x("DELETE FROM ad_images WHERE ad_id = ?", (ad_id,))
        self._xmany(
            "INSERT INTO ad_images (ad_id, position, url) VALUES (?, ?, ?)",
            [(ad_id, i, u) for i, u in enumerate(urls)],
        )

    def mark_missing(self, marketplace_id: int, run_start: str, now: str, threshold: int) -> tuple[int, int]:
        """Nach einem VOLLSTÄNDIGEN Full-Lauf: nicht gesehene Anzeigen bekommen einen Fehlzähler,
        erst ab `threshold` Läufen in Folge werden sie inaktiv. Gibt (gefehlt, inaktiv) zurück."""
        try:
            missed = self._x(
                """UPDATE ads SET missed_runs = missed_runs + 1
                   WHERE marketplace_id = ? AND is_active = 1 AND last_seen_at < ?""",
                (marketplace_id, run_start),
            ).rowcount
            inactive = self._x(
                """UPDATE ads SET is_active = 0, inactive_since = ?
                   WHERE marketplace_id = ? AND is_active = 1 AND missed_runs >= ?""",
                (now, marketplace_id, threshold),
            ).rowcount
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return missed, inactive

    # ---- Läufe ------------------------------------------------------------ #
    def start_run(self, marketplace_id: int, mode: str, started: str) -> int:
        row = self._x(
            """INSERT INTO scrape_runs (marketplace_id, mode, started_at, status)
               VALUES (?, ?, ?, 'running') RETURNING id""",
            (marketplace_id, mode, started),
        ).fetchone()
        self.conn.commit()
        return row["id"]

    def finish_run(self, run_id: int, rs: "RunStats") -> None:
        self._x(
            """UPDATE scrape_runs SET finished_at=?, pages=?, ads_seen=?, ads_new=?, ads_updated=?,
                   price_changes=?, ads_marked_inactive=?, rows_found=?, complete=?, status=?,
                   stop_reason=? WHERE id=?""",
            (utcnow(), rs.pages, rs.seen, rs.new, rs.updated, rs.price_changes, rs.marked_inactive,
             rs.rows_found, int(rs.complete), rs.status, rs.reason, run_id),
        )
        self.conn.commit()

    def ensure_api_grants(self, role: str) -> None:
        """PostgreSQL: die API-Rolle darf Aufträge anlegen/ändern (Lesen ist per Default-Rechte geregelt).
        Existiert die Rolle nicht, passiert nichts."""
        if not self.pg or not role or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", role):
            return
        self._x(
            f"""DO $$ BEGIN
                  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                    EXECUTE 'GRANT SELECT, INSERT, UPDATE ON enrichment_tasks TO {role}';
                  END IF;
                END $$""")
        self.conn.commit()

    def touch_marketplace(self, marketplace_id: int, full: bool) -> None:
        now = utcnow()
        if full:
            self._x("UPDATE marketplaces SET last_scraped_at=?, last_full_scrape_at=? WHERE id=?",
                    (now, now, marketplace_id))
        else:
            self._x("UPDATE marketplaces SET last_scraped_at=? WHERE id=?", (now, marketplace_id))
        self.conn.commit()


# --------------------------------------------------------------------------- #
# Teil 1: Kategorien
# --------------------------------------------------------------------------- #
@dataclass
class CategoryLink:
    id: int
    name: str
    slug: str
    href: str
    count: Optional[int] = None


_ID_RE = re.compile(r"-(\d+)$")


def parse_category_links(html: str) -> list[CategoryLink]:
    """Liest alle <a data-testid="category-link-…"> einer Seite (ohne 'Anzeigen in meiner Nähe')."""
    soup = BeautifulSoup(html, "html.parser")
    found: dict[int, CategoryLink] = {}
    for a in soup.select('a[data-testid^="category-link-"]'):
        href = a.get("href", "")
        path = href.split("?")[0].split("#")[0].rstrip("/")
        m = _ID_RE.search(path)
        if not m:
            continue
        span = a.find("span")
        name = span.get_text(strip=True) if span else ""
        if not name or "nähe" in name.lower():
            continue
        count = None
        count_el = a.find("span", attrs={"aria-label": re.compile("Treffer")})
        if count_el:
            count = to_int_count(count_el.get("aria-label", "")) or to_int_count(count_el.get_text())
        cid = int(m.group(1))
        found.setdefault(cid, CategoryLink(cid, name, path.rsplit("/", 1)[-1], href, count))
    return list(found.values())


def _save_debug(cfg: dict, name: str, content: str) -> Path:
    d = Path(cfg["debug_dir"])
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{re.sub(r'[^A-Za-z0-9_.-]', '_', name)}.html"
    p.write_text(content, encoding="utf-8")
    return p


def _abs(cfg: dict, href: str) -> str:
    return href if href.startswith("http") else cfg["base_url"].rstrip("/") + href


def update_categories(client: HttpClient, db: Storage, cfg: dict, only_main: Optional[list[str]] = None) -> None:
    log.info("Teil 1: Hauptkategorien von %s laden …", cfg["root_url"])
    html = client.get_text(cfg["root_url"])
    mains = parse_category_links(html)
    if not mains:
        p = _save_debug(cfg, "root_page", html)
        raise ParseError(
            f"Keine Hauptkategorien gefunden. Evtl. wird die Seite per JavaScript nachgeladen "
            f"oder das HTML hat sich geändert. Rohantwort gespeichert in: {p}"
        )
    main_ids = {m.id for m in mains}
    log.info("%d Hauptkategorien gefunden", len(mains))
    for m in mains:
        db.upsert_main(m, _abs(cfg, m.href))
    db.conn.commit()

    if only_main:
        wanted = {w.casefold() for w in only_main}
        mains = [m for m in mains if str(m.id) in wanted or m.name.casefold() in wanted]
        log.info("Eingeschränkt auf %d Hauptkategorien: %s", len(mains), ", ".join(m.name for m in mains))

    # --- Unterkategorien ---------------------------------------------------
    subs: list[tuple[CategoryLink, CategoryLink]] = []        # (main, sub)
    failed: list[str] = []
    for i, m in enumerate(mains, 1):
        try:
            page = client.get_text(_abs(cfg, m.href))
        except RequestFailed as e:
            log.error("Hauptkategorie '%s' übersprungen: %s", m.name, e)
            failed.append(m.name)
            continue
        links = [l for l in parse_category_links(page) if l.id not in main_ids and l.id != m.id]
        if not links:
            log.warning("[%d/%d] %s: keine Unterkategorien gefunden", i, len(mains), m.name)
            _save_debug(cfg, f"main_{m.id}", page)
            continue
        for s in links:
            db.upsert_sub(s, m.id, _abs(cfg, s.href))
            subs.append((m, s))
        db.conn.commit()
        log.info("[%d/%d] %s: %d Unterkategorien", i, len(mains), m.name, len(links))

    sub_ids = {s.id for _, s in subs}
    skip_ids = main_ids | sub_ids

    # --- Marktplätze -------------------------------------------------------
    total_mp = 0
    for i, (m, s) in enumerate(subs, 1):
        try:
            page = client.get_text(_abs(cfg, s.href))
        except RequestFailed as e:
            log.error("Unterkategorie '%s' übersprungen: %s", s.name, e)
            failed.append(s.name)
            continue
        links = [l for l in parse_category_links(page) if l.id not in skip_ids]
        if not links:
            # Unterkategorie ohne weitere Ebene -> sie selbst ist der Marktplatz
            links = [s]
            log.debug("%s hat keine tiefere Ebene, wird selbst als Marktplatz verwendet", s.name)
        for mp in links:
            db.upsert_marketplace(mp, m.id, s.id, _abs(cfg, mp.href))
        db.conn.commit()
        total_mp += len(links)
        log.info("[%d/%d] %s > %s: %d Marktplätze", i, len(subs), m.name, s.name, len(links))

    log.info("Fertig: %d Hauptkategorien, %d Unterkategorien, %d Marktplätze.",
             len(mains), len(subs), total_mp)
    if failed:
        log.warning("Bei diesen Kategorien gab es Fehler (bitte Befehl wiederholen): %s", ", ".join(failed))


# --------------------------------------------------------------------------- #
# Teil 2: Anzeigen
# --------------------------------------------------------------------------- #
def extract_summaries(data: dict) -> list[dict]:
    if "advertSummaryList" not in data:
        raise ParseError(
            "Antwort enthält kein 'advertSummaryList' – evtl. geänderte API oder Blockierung. "
            f"Top-Level-Schlüssel: {list(data)[:10]}"
        )
    return (data["advertSummaryList"] or {}).get("advertSummary") or []


def parse_ad(summary: dict, cfg: dict) -> dict:
    attrs: dict[str, Optional[str]] = {}
    for a in (summary.get("attributes") or {}).get("attribute", []):
        vals = a.get("values") or []
        attrs[a.get("name", "")] = vals[0] if vals else None

    ad_id = int(summary.get("id") or attrs.get("ADID") or 0)
    if not ad_id:
        raise ParseError("Anzeige ohne ID")

    street, house, door = parse_address(attrs.get("ADDRESS"))
    lat = lon = None
    if attrs.get("COORDINATES") and "," in attrs["COORDINATES"]:
        a, b = attrs["COORDINATES"].split(",", 1)
        lat, lon = to_float(a), to_float(b)

    price = to_float(attrs.get("PRICE/AMOUNT"))
    if price is None:
        price = to_float(attrs.get("PRICE"))

    images: list[str] = []
    base = cfg["image_base_url"]
    for ref in (attrs.get("ALL_IMAGE_URLS") or "").split(";"):
        if ref.strip():
            images.append(base + ref.strip())
    if not images:
        for img in (summary.get("advertImageList") or {}).get("advertImage", []):
            u = img.get("referenceImageUrl") or img.get("mainImageUrl")
            if u and u not in images:
                images.append(u)

    seo = attrs.get("SEO_URL")
    url = f"{cfg['base_url']}/iad/{seo}" if seo else f"{cfg['base_url']}/iad/object?adId={ad_id}"

    return {
        "id": ad_id,
        "uuid": attrs.get("AD_UUID"),
        "category_tree_ids": attrs.get("categorytreeids"),
        "url": url,
        "title": attrs.get("HEADING") or summary.get("description"),
        "description": attrs.get("BODY_DYN"),
        "price": price,
        "price_display": attrs.get("PRICE_FOR_DISPLAY"),
        "street": street,
        "house_number": house,
        "door": door,
        "postcode": attrs.get("POSTCODE"),
        "city": attrs.get("LOCATION"),
        "district": attrs.get("DISTRICT"),
        "state": attrs.get("STATE"),
        "address_raw": attrs.get("ADDRESS"),
        "latitude": lat,
        "longitude": lon,
        "is_private": (1 if attrs.get("ISPRIVATE") == "1" else 0) if attrs.get("ISPRIVATE") is not None else None,
        "org_id": attrs.get("ORGID"),
        "status": (summary.get("advertStatus") or {}).get("id"),
        "published_at": ms_to_iso(attrs.get("PUBLISHED")),
        "changed_at": ms_to_iso(attrs.get("CHANGED")),
        "end_date": ms_to_iso(attrs.get("ENDDATE")),
        "images": images,
    }


@dataclass
class RunStats:
    pages: int = 0
    seen: int = 0
    new: int = 0
    updated: int = 0
    unchanged: int = 0
    duplicates: int = 0
    price_changes: int = 0
    marked_inactive: int = 0
    missed: int = 0
    parse_errors: int = 0
    rows_found: Optional[int] = None
    complete: bool = False
    status: str = "ok"
    reason: str = ""


def build_buckets(level: dict) -> list[tuple[str, dict]]:
    """Wandelt eine split_levels-Definition in (Name, Query-Parameter)-Paare um."""
    kind = level.get("type")
    if kind == "values":
        return [(f"{k}", {level["param"]: v}) for k, v in level["values"].items()]
    if kind == "range":
        edges = level["edges"]
        offset = level.get("to_offset", 1)
        out = []
        for i, lo in enumerate(edges):
            params = {level["param_from"]: lo}
            if i + 1 < len(edges):
                hi = round(edges[i + 1] - offset, 2)
                hi = int(hi) if float(hi).is_integer() else hi
                params[level["param_to"]] = hi
                name = f"{lo}-{hi}"
            else:
                name = f"ab {lo}"
            out.append((f"{level['name']} {name}", params))
        return out
    raise ScraperError(f"Unbekannter split_levels-Typ: {kind!r}")


@dataclass
class PartResult:
    complete: bool
    found: Optional[int] = None
    reason: str = ""


@dataclass
class _Ctx:
    mp: dict
    mode: str
    url: str
    rs: "RunStats"
    seen: set = field(default_factory=set)


class Scraper:
    def __init__(self, client: HttpClient, db: Storage, cfg: dict):
        self.client, self.db, self.cfg = client, db, cfg

    def scrape_marketplace(self, mp: dict, mode: str) -> RunStats:
        cfg = self.cfg
        run_start = utcnow()
        rs = RunStats()
        run_id = self.db.start_run(mp["id"], mode, run_start)
        ctx = _Ctx(mp, mode, cfg["search_url_template"].format(slug=mp["slug"]), rs)
        label = mp["name"]
        levels = cfg.get("split_levels", []) if cfg.get("split_enabled") else []

        try:
            part = self._scrape_query(ctx, {}, levels, label, None)
            rs.reason = part.reason
            parts_ok = part.complete
        except KeyboardInterrupt:
            rs.status, rs.reason = "interrupted", "keyboard_interrupt"
            self.db.finish_run(run_id, rs)
            raise
        except ScraperError as e:
            rs.status, rs.reason = "failed", str(e)[:200]
            self.db.finish_run(run_id, rs)
            raise
        except Exception as e:                          # unerwartet -> sauber protokollieren
            rs.status, rs.reason = "failed", f"{e.__class__.__name__}: {e}"[:200]
            self.db.finish_run(run_id, rs)
            raise ScraperError(f"Unerwarteter Fehler: {e.__class__.__name__}: {e}") from e

        # Vollständigkeit prüfen: nur dann dürfen fehlende Anzeigen gezählt/inaktiv gesetzt werden
        tol = cfg["completeness_tolerance"]
        enough = rs.rows_found is None or rs.seen >= rs.rows_found * (1 - tol)
        rs.complete = parts_ok and enough
        if mode == "full" and not rs.complete:
            log.warning(
                "[%s] UNVOLLSTÄNDIG (%s): %d von %s Anzeigen geholt (API-Limit %d Seiten, max_pages oder "
                "Aufteilung unzureichend) – es werden KEINE Anzeigen als fehlend gezählt.",
                label, rs.reason, rs.seen, rs.rows_found, API_MAX_PAGES)
        if mode == "full" and rs.complete:
            rs.missed, rs.marked_inactive = self.db.mark_missing(
                mp["id"], run_start, utcnow(), cfg["inactive_after_missed_runs"])
            if rs.missed:
                log.info("[%s] %d aktive Anzeigen in diesem Lauf nicht gesehen, davon %d jetzt inaktiv "
                         "(Regel: %d Läufe in Folge)", label, rs.missed, rs.marked_inactive,
                         cfg["inactive_after_missed_runs"])

        self.db.touch_marketplace(mp["id"], full=(mode == "full" and rs.complete))
        self.db.finish_run(run_id, rs)
        return rs

    def _scrape_query(self, ctx: _Ctx, filters: dict, levels: list, label: str,
                      parent_found: Optional[int]) -> PartResult:
        """Blättert eine (ggf. gefilterte) Suche durch. Ist sie größer als das API-Limit,
        wird sie im Modus full nach levels[0] aufgeteilt."""
        cfg, rs = self.cfg, ctx.rs
        rows = cfg["rows_per_page"]
        max_pages = min(cfg["max_pages"], API_MAX_PAGES)
        cap = API_MAX_PAGES * rows
        found: Optional[int] = None

        for page in range(1, max_pages + 1):
            if STOP_EVENT.is_set():
                raise KeyboardInterrupt
            params = {"rows": rows, "page": page, **cfg.get("extra_params", {}), **filters}
            data = self.client.get_json(ctx.url, params, referer=ctx.mp.get("url"))
            summaries = extract_summaries(data)
            rs.pages += 1
            if data.get("rowsFound") is not None:
                found = int(data["rowsFound"])
                if page == 1 and not filters:
                    rs.rows_found = found

            if page == 1:
                if filters and parent_found is not None and found is not None and found >= parent_found:
                    raise ScraperError(
                        f"Filter {filters} wirkt nicht (rowsFound {found} >= {parent_found}). "
                        f"Parameternamen in 'split_levels' prüfen (Befehl: splitcheck {ctx.mp['id']}).")
                if ctx.mode == "full" and found is not None and found > cap:
                    if levels:
                        log.info("[%s] %d Anzeigen > API-Limit (%d) -> Aufteilung nach '%s'",
                                 label, found, cap, levels[0].get("name"))
                        return self._split(ctx, filters, levels, label, found)
                    log.warning("[%s] %d Anzeigen > API-Limit (%d) und keine weitere Aufteilung möglich – "
                                "nur die ersten %d werden geholt", label, found, cap, cap)

            if not summaries:
                return PartResult(True, found, "empty_page")

            parsed: list[dict] = []
            for sm in summaries:
                try:
                    parsed.append(parse_ad(sm, cfg))
                except (ScraperError, ValueError, TypeError, AttributeError) as e:
                    rs.parse_errors += 1
                    log.warning("[%s] Anzeige übersprungen (%s): %s", label, e, str(sm.get("id")))

            fresh: list[dict] = []
            for ad in parsed:                      # Duplikate (Verschiebungen zwischen Seiten) verwerfen
                if ad["id"] in ctx.seen:
                    rs.duplicates += 1
                else:
                    ctx.seen.add(ad["id"])
                    fresh.append(ad)

            if not fresh:
                return PartResult(False, found, "duplicate_page")

            known = self.db.known_ids([a["id"] for a in fresh])
            unknown = sum(1 for a in fresh if a["id"] not in known)
            c = self.db.save_ads(fresh, ctx.mp["id"], utcnow())
            rs.new += c["new"]
            rs.updated += c["updated"]
            rs.unchanged += c["unchanged"]
            rs.price_changes += c["price_changes"]
            rs.seen = len(ctx.seen)

            log.info("[%s] Seite %d: %d Items | neu %d, geändert %d | Preisänd. %d | gesamt %d/%s",
                     label, page, len(summaries), c["new"], c["updated"], c["price_changes"],
                     rs.seen, rs.rows_found if rs.rows_found is not None else "?")

            if len(summaries) < cfg["stop_below_items"]:
                return PartResult(True, found, "last_page")
            if found is not None and page * rows >= found:
                return PartResult(True, found, "rows_found_reached")
            if ctx.mode == "incremental" and unknown < cfg["incremental_min_new_per_page"]:
                return PartResult(False, found, "caught_up")
        return PartResult(False, found, "max_pages")

    def _split(self, ctx: _Ctx, filters: dict, levels: list, label: str, parent_found: int) -> PartResult:
        level, rest = levels[0], levels[1:]
        tol = self.cfg["completeness_tolerance"]
        all_ok, child_total = True, 0
        for name, extra in build_buckets(level):
            part = self._scrape_query(ctx, {**filters, **extra}, rest, f"{label} / {name}", parent_found)
            all_ok &= part.complete
            child_total += part.found or 0
        if child_total < parent_found * (1 - tol):
            log.warning("[%s] Aufteilung nach '%s' deckt nur %d von %d Anzeigen ab (Anzeigen ohne Preis/"
                        "Region?) -> Teil gilt als unvollständig", label, level.get("name"),
                        child_total, parent_found)
            all_ok = False
        elif child_total > parent_found * (1 + tol):
            log.warning("[%s] Teile der Aufteilung nach '%s' überlappen (%d > %d) – Duplikate werden verworfen",
                        label, level.get("name"), child_total, parent_found)
        return PartResult(all_ok, parent_found, "split")

    def run(self, marketplaces: list[dict], mode: str) -> int:
        """Gibt Anzahl fehlgeschlagener Marktplätze zurück."""
        total: Counter = Counter()
        failures: list[str] = []
        consecutive = 0
        try:
            for i, mp in enumerate(marketplaces, 1):
                if STOP_EVENT.is_set():
                    raise KeyboardInterrupt
                log.info("=== [%d/%d] %s > %s > %s (Modus: %s) ===", i, len(marketplaces),
                         mp["main_name"], mp["sub_name"], mp["name"], mode)
                try:
                    rs = self.scrape_marketplace(mp, mode)
                except ScraperError as e:
                    log.error("[%s] FEHLER: %s", mp["name"], e)
                    failures.append(mp["name"])
                    consecutive += 1
                    limit = self.cfg["max_consecutive_failures"]
                    if limit and consecutive >= limit:
                        left = len(marketplaces) - i
                        log.error("ABBRUCH: %d Fehler in Folge – möglicherweise Blockade oder Netzproblem. "
                                  "%d Marktplätze wurden nicht bearbeitet.", consecutive, left)
                        failures.append(f"(Abbruch nach {consecutive} Fehlern in Folge)")
                        break
                    continue
                consecutive = 0
                for k in ("pages", "seen", "new", "updated", "price_changes", "marked_inactive", "duplicates"):
                    total[k] += getattr(rs, k)
                log.info("[%s] fertig (%s): %d Anzeigen, %d neu, %d geändert, %d Preisänderungen",
                         mp["name"], rs.reason, rs.seen, rs.new, rs.updated, rs.price_changes)
        except KeyboardInterrupt:
            log.warning("Lauf abgebrochen – bisher gespeicherte Daten bleiben erhalten.")
            failures.append("(abgebrochen)")

        log.info("-" * 60)
        log.info("Gesamt: %d Seiten | %d Anzeigen | %d neu | %d geändert | %d Preisänderungen | "
                 "%d inaktiv | %d Duplikate verworfen",
                 total["pages"], total["seen"], total["new"], total["updated"],
                 total["price_changes"], total["marked_inactive"], total["duplicates"])
        if failures:
            log.error("Fehlgeschlagen/abgebrochen: %s", ", ".join(failures))
        return len(failures)


# --------------------------------------------------------------------------- #
# Kategorie-Auswahl
# --------------------------------------------------------------------------- #
def select_marketplaces(db: Storage, includes: list[str], excludes: list[str]) -> list[dict]:
    all_mps = db.marketplaces()
    if not all_mps:
        raise ScraperError("Keine Marktplätze in der Datenbank. Zuerst 'categories' ausführen.")

    def matches(mp: dict, token: str) -> bool:
        t = token.strip()
        if t.isdigit():
            return int(t) in (mp["id"], mp["sub_category_id"], mp["main_category_id"])
        t = t.casefold()
        return t in (mp["name"].casefold(), mp["sub_name"].casefold(), mp["main_name"].casefold())

    def pick(tokens: list[str]) -> list[dict]:
        out: dict[int, dict] = {}
        for tok in tokens:
            hits = [m for m in all_mps if matches(m, tok)]
            if not hits:
                log.warning("Kategorie '%s' nicht gefunden (siehe Befehl 'list')", tok)
            for m in hits:
                out[m["id"]] = m
        return list(out.values())

    selected = pick(includes) if includes else list(all_mps)
    if includes and not selected:
        raise ScraperError("Keine der angegebenen Kategorien wurde gefunden.")
    excluded = {m["id"] for m in pick(excludes)} if excludes else set()
    return [m for m in selected if m["id"] not in excluded]


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def split_arg(value: Optional[str]) -> Optional[list[str]]:
    if value is None:
        return None
    return [v.strip() for v in value.split(",") if v.strip()]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="willhaben.at Scraper")
    p.add_argument("--config", help="Pfad zur config.json (Standard: ./config.json)")
    p.add_argument("--db", help="Pfad zur SQLite-Datei")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug-Ausgaben")
    p.add_argument("--proxy", metavar="URL", help="Proxy einschalten, z.B. http://user:pass@host:8080 (überschreibt config)")
    p.add_argument("--no-proxy", action="store_true", help="Proxy für diesen Aufruf ausschalten")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("categories", help="Teil 1: Kategorien von willhaben laden")
    c.add_argument("--nur-haupt", help="Nur diese Hauptkategorien (IDs/Namen, Komma-getrennt)")

    sub.add_parser("proxytest", help="Proxy-Einstellungen testen (IP direkt und über Proxy, willhaben erreichbar?)")

    s = sub.add_parser("scrape", help="Teil 2: Anzeigen holen")
    s.add_argument("--kategorien", help="IDs oder Namen, Komma-getrennt (überschreibt config)")
    s.add_argument("--ausschliessen", help="IDs oder Namen, die übersprungen werden")
    s.add_argument("--mode", choices=["full", "incremental"])
    s.add_argument("--delay", type=float, help="Sekunden zwischen Anfragen")
    s.add_argument("--rows", type=int, help="rows-Parameter (Anzeigen pro Seite)")
    s.add_argument("--stop-below", type=int, help="Seite mit weniger Items = letzte Seite")
    s.add_argument("--max-pages", type=int, help="Seiten-Limit pro Marktplatz")

    ls = sub.add_parser("list", help="Gespeicherte Marktplätze anzeigen")
    ls.add_argument("-o", "--output", metavar="DATEI",
                    help="Ausgabe zusätzlich in diese Textdatei schreiben (Pfad relativ zum aktuellen Ordner)")

    pr = sub.add_parser("probe", help="Eine einzelne Suchseite abfragen (zum Testen)")
    pr.add_argument("category", help="Marktplatz-ID oder Name")
    pr.add_argument("--page", type=int, default=1)
    pr.add_argument("--param", action="append", default=[], metavar="KEY=WERT",
                    help="zusätzlicher Query-Parameter, mehrfach möglich (z.B. --param areaId=900)")

    sc = sub.add_parser("splitcheck", help="Prüft die split_levels-Parameter anhand eines Marktplatzes")
    sc.add_argument("category", help="Marktplatz-ID oder Name (am besten ein großer, z.B. 4000)")
    return p


def cmd_list(db: Storage, output: Optional[str] = None) -> None:
    mps = db.marketplaces()
    if not mps:
        print("Keine Marktplätze gespeichert. Zuerst: python willhaben_scraper.py categories")
        return
    lines = [f"{'ID':>8}  {'Anzeigen':>9}  {'in DB':>7}  Pfad"]
    for m in mps:
        cnt = f"{m['ad_count']:,}".replace(",", ".") if m["ad_count"] is not None else "?"
        lines.append(f"{m['id']:>8}  {cnt:>9}  {m['db_active_ads']:>7}  "
                     f"{m['main_name']} > {m['sub_name']} > {m['name']}")
    lines += ["", f"{len(mps)} Marktplätze"]
    text = "\n".join(lines)
    print(text)
    if output:
        try:
            Path(output).expanduser().write_text(text + "\n", encoding="utf-8")
        except OSError as e:
            raise ScraperError(f"Textdatei konnte nicht geschrieben werden ({output}): {e}") from e
        print(f"\nGespeichert in: {Path(output).expanduser().resolve()}")


def cmd_probe(client: HttpClient, db: Storage, cfg: dict, token: str, page: int,
              extra: Optional[list[str]] = None) -> None:
    mp = select_marketplaces(db, [token], [])[0]
    url = cfg["search_url_template"].format(slug=mp["slug"])
    extra_params = {}
    for item in extra or []:
        if "=" not in item:
            raise ScraperError(f"--param erwartet KEY=WERT, bekam: {item!r}")
        k, v = item.split("=", 1)
        extra_params[k] = v
    data = client.get_json(url, {"rows": cfg["rows_per_page"], "page": page,
                                 **cfg.get("extra_params", {}), **extra_params})
    items = extract_summaries(data)
    print(f"Marktplatz     : {mp['name']} ({mp['slug']})")
    print(f"Seite          : {page} (angefordert: {data.get('pageRequested')})")
    print(f"rowsFound      : {data.get('rowsFound')}")
    print(f"rowsReturned   : {data.get('rowsReturned')}   Items in Antwort: {len(items)}")
    for s in items[:3]:
        ad = parse_ad(s, cfg)
        print(f"  Beispiel     : {ad['id']} | {ad['title']} | {ad['price_display']} | {ad['postcode']} {ad['city']}")


def cmd_proxytest(cfg: dict) -> int:
    """Zeigt die öffentliche IP direkt und über den Proxy und prüft, ob willhaben über den Proxy erreichbar ist."""
    ip_url = "https://api.ipify.org"
    ua = {"User-Agent": cfg["user_agent"]}

    def fetch(label: str, url: str, proxy: Optional[str]) -> bool:
        sess = requests.Session()
        sess.trust_env = False
        if proxy:
            sess.proxies = {"http": proxy, "https": proxy}
        try:
            r = sess.get(url, headers=ua, timeout=cfg["timeout_seconds"])
        except requests.RequestException as e:
            msg = str(e)
            if proxy:
                for sec in {proxy, urlsplit(proxy).password or ""}:
                    if sec:
                        msg = msg.replace(sec, "***")
            print(f"  {label:<22}: FEHLER {e.__class__.__name__}: {msg[:200]}")
            return False
        body = r.text.strip()[:80] if "ipify" in url else ""
        print(f"  {label:<22}: HTTP {r.status_code} {body}")
        return r.status_code == 200

    print("Direkt (ohne Proxy):")
    fetch("öffentliche IP", ip_url, None)
    if not cfg.get("proxy_enabled"):
        print("\nProxy ist ausgeschaltet (proxy_enabled=false).")
        return 0
    proxy = proxy_url_for(cfg, "search")
    if not proxy:
        print("\nProxy ist eingeschaltet, gilt aber nicht für 'search' (proxy_use_for).")
        return 1
    print(f"\nÜber Proxy {redact_proxy(proxy)}:")
    ok = fetch("öffentliche IP", ip_url, proxy)
    ok = fetch("willhaben.at", "https://www.willhaben.at/", proxy) and ok
    print("\nErgebnis:", "Proxy funktioniert." if ok else "Proxy-Test fehlgeschlagen.")
    return 0 if ok else 1


def cmd_splitcheck(client: HttpClient, db: Storage, cfg: dict, token: str) -> None:
    """Fragt für jede Aufteilungs-Stufe je Teil die Trefferzahl ab und vergleicht die Summe mit dem Gesamtwert."""
    mp = select_marketplaces(db, [token], [])[0]
    url = cfg["search_url_template"].format(slug=mp["slug"])
    rows, cap = cfg["rows_per_page"], API_MAX_PAGES * cfg["rows_per_page"]

    def found_for(extra: dict) -> int:
        data = client.get_json(url, {"rows": rows, "page": 1, **cfg.get("extra_params", {}), **extra},
                               referer=mp.get("url"))
        extract_summaries(data)
        return int(data.get("rowsFound") or 0)

    total = found_for({})
    print(f"Marktplatz {mp['name']} ({mp['id']}): rowsFound gesamt = {total} (API-Limit pro Abfrage: {cap})")
    for level in cfg.get("split_levels", []):
        buckets = build_buckets(level)
        print(f"\nStufe '{level['name']}' ({len(buckets)} Teile):")
        s = 0
        for name, extra in buckets:
            f = found_for(extra)
            s += f
            flag = "   <-- über dem Limit, wird weiter aufgeteilt" if f > cap else ""
            print(f"  {name:<28} {f:>10}{flag}")
        diff = (s - total) / total * 100 if total else 0.0
        verdict = "OK" if abs(diff) <= cfg["completeness_tolerance"] * 100 else "PRÜFEN (Filter greift nicht/Lücken/Überlappung)"
        print(f"  {'Summe':<28} {s:>10}  ({diff:+.1f} % zum Gesamtwert) -> {verdict}")
        if level.get("type") == "range":
            # Diagnose: Wo liegt die Lücke? (alle Anzeigen mit Preis vs. Summe der Stufen vs. Gesamtwert)
            first = level["edges"][0]
            priced = found_for({level["param_from"]: first})
            print(f"\n  Diagnose Preisfilter:")
            print(f"    Alle mit Preis ({level['param_from']}={first}) : {priced:>10}")
            print(f"    -> von den Stufen nicht erfasst (z.B. Cent-Preise): {priced - s:>10}")
            print(f"    -> ohne Preis (per Preisfilter nicht erreichbar)  : {total - priced:>10}")


def touch_heartbeat(cfg: dict) -> None:
    """Berührt die Heartbeat-Datei (Docker-Healthcheck prüft deren Alter)."""
    path = cfg.get("heartbeat_file")
    if not path:
        return
    try:
        Path(path).touch()
    except OSError as e:                       # pragma: no cover
        log.debug("Heartbeat nicht schreibbar: %s", e)


def lock_path(cfg: dict, name: str) -> str:
    """willhaben.lock -> willhaben.<name>.lock (getrennte Sperren für full, incremental, categories)."""
    p = Path(cfg["lock_file"])
    return str(p.with_name(f"{p.stem}.{name}{p.suffix}"))


def read_db_password(cfg: dict) -> Optional[str]:
    pf = cfg.get("db_password_file")
    if pf:
        try:
            return Path(pf).read_text(encoding="utf-8").strip()
        except OSError as e:
            raise ScraperError(f"Passwort-Datei nicht lesbar ({pf}): {e}") from e
    return os.environ.get("PGPASSWORD")


def open_storage(cfg: dict) -> Storage:
    if cfg.get("db_backend") == "postgres":
        pg = {"host": cfg["db_host"], "port": int(cfg["db_port"]),
              "dbname": cfg["db_name"], "user": cfg["db_user"]}
        password = read_db_password(cfg)
        if password:
            pg["password"] = password
        return Storage(dialect="postgres", pg=pg)
    return Storage(cfg["db_path"])


def normalize_paths(cfg: dict) -> None:
    for key in ("db_path", "debug_dir", "log_file", "lock_file", "head_media_dir", "rate_state_file",
                "proxy_password_file"):
        if cfg.get(key):
            cfg[key] = resolve_path(cfg[key])


def setup_logging(cfg: dict, verbose: bool = False, write_file: bool = True) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if cfg.get("log_file") and write_file:
        handlers.append(logging.handlers.RotatingFileHandler(
            cfg["log_file"], maxBytes=cfg["log_max_bytes"], backupCount=cfg["log_backups"], encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if verbose else getattr(logging, str(cfg["log_level"]).upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S", handlers=handlers, force=True,
    )


def run_categories(cfg: dict, only_main: Optional[list[str]] = None) -> int:
    lock = RunLock(lock_path(cfg, "categories")) if cfg.get("lock_file") else None
    if lock and not lock.acquire():
        log.warning("Ein Kategorien-Lauf ist noch aktiv – dieser Start wird übersprungen.")
        return 0
    try:
        db = open_storage(cfg)
        try:
            update_categories(HttpClient(cfg), db, cfg, only_main)
        finally:
            db.close()
        return 0
    finally:
        if lock:
            lock.release()


def run_scrape(cfg: dict, mode: Optional[str] = None) -> int:
    """Ein kompletter Lauf (full oder incremental). Gibt 0 bei Erfolg zurück, sonst 1."""
    cfg = dict(cfg)
    if mode:
        cfg["mode"] = mode
    mode = cfg["mode"]
    if mode not in ("full", "incremental"):
        raise ScraperError(f"Ungültiger mode: {mode}")

    # Pro Modus eine eigene Sperre: full und incremental dürfen gleichzeitig laufen,
    # aber nie zwei Läufe desselben Modus.
    lock = RunLock(lock_path(cfg, mode)) if cfg.get("lock_file") else None
    if lock and not lock.acquire():
        log.warning("Ein '%s'-Lauf ist noch aktiv (Lock: %s) – dieser Start wird übersprungen.",
                    mode, lock_path(cfg, mode))
        return 0
    try:
        db = open_storage(cfg)
        try:
            if cfg["max_pages"] < API_MAX_PAGES:
                log.warning("max_pages=%d ist kleiner als das API-Limit (%d): pro Abfrage werden nur %d "
                            "Anzeigen geholt – Marktplätze mit mehr Anzeigen bleiben unvollständig.",
                            cfg["max_pages"], API_MAX_PAGES, cfg["max_pages"] * cfg["rows_per_page"])
            mps = select_marketplaces(db, cfg["categories"], cfg["exclude_categories"])
            if mode == "incremental" and cfg.get("incremental_requires_full"):
                total = len(mps)
                mps = [m for m in mps if m.get("last_full_scrape_at")]
                if total - len(mps):
                    log.info("%d Marktplätze ohne abgeschlossenen Full-Lauf werden im Incremental-Lauf "
                             "übersprungen", total - len(mps))
                if not mps:
                    log.warning("Noch kein Marktplatz hat einen abgeschlossenen Full-Lauf – nichts zu tun.")
                    return 0
            if cfg["oldest_first"]:
                key = "last_full_scrape_at" if mode == "full" else "last_scraped_at"
                mps.sort(key=lambda m: (m[key] is not None, m[key] or ""))   # nie gescrapte zuerst
            if cfg.get("priority_categories"):
                prio = {m["id"] for m in select_marketplaces(db, cfg["priority_categories"], [])}
                mps.sort(key=lambda m: m["id"] not in prio)   # stabil: Reihenfolge innerhalb der Gruppen bleibt
                log.info("%d Marktplätze mit Vorrang werden zuerst bearbeitet",
                         sum(1 for m in mps if m["id"] in prio))
            log.info("%d Marktplätze ausgewählt, Pause %.1f s, rows=%d, Modus=%s",
                     len(mps), cfg["request_delay_seconds"], cfg["rows_per_page"], mode)
            return 1 if Scraper(HttpClient(cfg), db, cfg).run(mps, mode) else 0
        finally:
            db.close()
    finally:
        if lock:
            lock.release()


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)

    # Kommandozeile überschreibt Config und Umgebungsvariablen
    overrides = {
        "db_path": args.db,
        "mode": getattr(args, "mode", None),
        "request_delay_seconds": getattr(args, "delay", None),
        "rows_per_page": getattr(args, "rows", None),
        "stop_below_items": getattr(args, "stop_below", None),
        "max_pages": getattr(args, "max_pages", None),
        "categories": split_arg(getattr(args, "kategorien", None)),
        "exclude_categories": split_arg(getattr(args, "ausschliessen", None)),
    }
    cfg.update({k: v for k, v in overrides.items() if v is not None})
    if args.proxy:
        cfg.update(proxy_enabled=True, proxy_url=args.proxy)
    if args.no_proxy:
        cfg["proxy_enabled"] = False
    normalize_paths(cfg)
    setup_logging(cfg, verbose=args.verbose, write_file=args.command in ("categories", "scrape"))

    try:
        if args.command == "categories":
            return run_categories(cfg, split_arg(args.nur_haupt))
        if args.command == "scrape":
            return run_scrape(cfg)
        if args.command == "proxytest":
            return cmd_proxytest(cfg)
        db = open_storage(cfg)
        try:
            client = HttpClient(cfg)
            if args.command == "list":
                cmd_list(db, args.output)
            elif args.command == "probe":
                cmd_probe(client, db, cfg, args.category, args.page, args.param)
            elif args.command == "splitcheck":
                cmd_splitcheck(client, db, cfg, args.category)
        finally:
            db.close()
        return 0
    except ScraperError as e:
        log.error("%s", e)
        return 1
    except KeyboardInterrupt:
        log.warning("Abgebrochen.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
