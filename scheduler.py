#!/usr/bin/env python3
"""
scheduler.py – startet die Scraper-Läufe zeitgesteuert (ohne zusätzliche Bibliothek).

  * incremental   alle `schedule_incremental_minutes` Minuten (Standard 60)
  * full          wöchentlich (Standard: sonntags 02:00)
  * categories    monatlich (Standard: am 1. um 01:00)
  * beim Start    fehlen Kategorien oder Full-Läufe, werden sie sofort nachgeholt

Jeder Job läuft in einem eigenen Thread. Pro Job läuft höchstens eine Instanz: ist ein Lauf beim
nächsten Termin noch aktiv, wird der Termin übersprungen. Full und incremental laufen parallel;
das Rate-Limit (request_delay_seconds) gilt gemeinsam für alle Läufe im Prozess.

Einstellungen: config.json bzw. Umgebungsvariablen WH_<KEY> (siehe willhaben_scraper.py), z.B.
  WH_SCHEDULE_FULL='{"weekday":"sat","hour":3,"minute":0}'
  WH_SCHEDULE_INCREMENTAL_MINUTES=30
Beenden mit SIGTERM/SIGINT: laufende Läufe stoppen nach der aktuellen Seite.
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
from datetime import datetime, timedelta
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import willhaben_scraper as w

log = logging.getLogger("willhaben.scheduler")

WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def next_weekly(now: datetime, weekday: int, hour: int, minute: int) -> datetime:
    """Nächster Termin (weekday 0=Mo ... 6=So) nach `now`, in der Zeitzone von `now`."""
    cand = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    cand += timedelta(days=(weekday - cand.weekday()) % 7)
    if cand <= now:
        cand += timedelta(days=7)
    return cand


def next_monthly(now: datetime, day: int, hour: int, minute: int) -> datetime:
    """Nächster Termin am `day`. Tag eines Monats; Monate, die den Tag nicht haben, werden übersprungen."""
    year, month = now.year, now.month
    for _ in range(24):
        try:
            cand = now.replace(year=year, month=month, day=day, hour=hour, minute=minute,
                               second=0, microsecond=0)
            if cand > now:
                return cand
        except ValueError:
            pass
        month += 1
        if month > 12:
            month, year = 1, year + 1
    raise ValueError(f"Kein gültiger Termin für Tag {day}")


class Job:
    def __init__(self, name: str, func: Callable[[], int], schedule: Callable[[datetime], datetime],
                 first_due: datetime):
        self.name = name
        self.func = func
        self.schedule = schedule          # now -> nächster Termin
        self.next_due = first_due
        self.thread: Optional[threading.Thread] = None

    def running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


class Scheduler:
    def __init__(self, cfg: dict, now: Optional[datetime] = None):
        self.cfg = cfg
        self.tz = ZoneInfo(cfg["schedule_timezone"])
        now = now or datetime.now(self.tz)
        minutes = int(cfg["schedule_incremental_minutes"])
        full = cfg["schedule_full"]
        cats = cfg["schedule_categories"]
        weekday = WEEKDAYS[str(full.get("weekday", "sun")).lower()[:3]]

        incr_sched = lambda n: n + timedelta(minutes=minutes)
        full_sched = lambda n: next_weekly(n, weekday, int(full.get("hour", 2)), int(full.get("minute", 0)))
        cat_sched = lambda n: next_monthly(n, int(cats.get("day", 1)), int(cats.get("hour", 1)),
                                           int(cats.get("minute", 0)))
        self.jobs = [
            Job("incremental", lambda: w.run_scrape(cfg, "incremental"), incr_sched, incr_sched(now)),
            Job("full", lambda: w.run_scrape(cfg, "full"), full_sched, full_sched(now)),
            Job("categories", lambda: w.run_categories(cfg), cat_sched, cat_sched(now)),
        ]
        for j in self.jobs:
            log.info("Job '%s': nächster Start %s", j.name, j.next_due.strftime("%Y-%m-%d %H:%M %Z"))
        self.startup_thread: Optional[threading.Thread] = None

    # ---- Ausführung ------------------------------------------------------- #
    def _run_job(self, name: str, func: Callable[[], int]) -> None:
        log.info("Job '%s' startet", name)
        try:
            rc = func()
            (log.info if rc == 0 else log.error)("Job '%s' beendet (Exit-Code %s)", name, rc)
        except w.ScraperError as e:
            log.error("Job '%s' fehlgeschlagen: %s", name, e)
        except Exception:
            log.exception("Job '%s' mit unerwartetem Fehler beendet", name)

    def _start(self, job: Job) -> None:
        job.thread = threading.Thread(target=self._run_job, args=(job.name, job.func),
                                      name=job.name, daemon=False)
        job.thread.start()

    def tick(self, now: Optional[datetime] = None) -> None:
        now = now or datetime.now(self.tz)
        for job in self.jobs:
            if now < job.next_due:
                continue
            if job.running():
                log.warning("Job '%s' läuft noch – dieser Termin wird übersprungen", job.name)
            else:
                self._start(job)
            job.next_due = job.schedule(now)

    # ---- Start-Prüfung ---------------------------------------------------- #
    def startup_check(self) -> None:
        """Fehlen Kategorien oder Full-Läufe, werden sie sofort nachgeholt (eigener Thread)."""
        def work() -> None:
            db = w.open_storage(self.cfg)
            try:
                total, never_full = db.marketplace_counts()
            finally:
                db.close()
            if total == 0:
                self._run_job("categories", lambda: w.run_categories(self.cfg))
            if self.cfg.get("full_on_start_if_incomplete") and (total == 0 or never_full > 0):
                log.info("%s Marktplätze ohne abgeschlossenen Full-Lauf -> Full-Lauf beim Start",
                         "alle" if total == 0 else never_full)
                self._run_job("full (Start)", lambda: w.run_scrape(self.cfg, "full"))

        def safe() -> None:
            try:
                work()
            except Exception:
                log.exception("Start-Prüfung fehlgeschlagen")

        self.startup_thread = threading.Thread(target=safe, name="startup", daemon=False)
        self.startup_thread.start()

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            w.touch_heartbeat(self.cfg)
            self.tick()
            stop.wait(10)

    def wait_for_jobs(self, timeout: float) -> None:
        threads = [j.thread for j in self.jobs if j.thread] + ([self.startup_thread] if self.startup_thread else [])
        deadline = datetime.now() + timedelta(seconds=timeout)
        for t in threads:
            remaining = (deadline - datetime.now()).total_seconds()
            if t.is_alive() and remaining > 0:
                t.join(remaining)


def main() -> int:
    cfg = w.load_config(None)
    w.normalize_paths(cfg)
    w.setup_logging(cfg, write_file=True)

    stop = threading.Event()

    def on_signal(signum, _frame):
        log.warning("Signal %s empfangen – Läufe werden nach der aktuellen Seite beendet", signum)
        w.STOP_EVENT.set()
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    scheduler = Scheduler(cfg)
    scheduler.startup_check()
    scheduler.run_forever(stop)
    scheduler.wait_for_jobs(timeout=60)
    log.info("Scheduler beendet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
