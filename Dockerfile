# Ein Image für Scraper/Scheduler UND Kopf (unterschiedlicher Startbefehl in docker-compose.yml)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Europe/Vienna

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY willhaben_scraper.py scheduler.py head.py healthcheck.py config.json ./

# Kein root im Container. /data: Locks und Debug-Dateien, /media: heruntergeladene Bilder
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app \
 && mkdir -p /data /media \
 && chown app:app /data /media
USER app

CMD ["python", "scheduler.py"]
