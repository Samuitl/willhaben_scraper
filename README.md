# willhaben-System (Docker)

Drei Container, alle auf einem Linux-Server:

```
                  ┌───────────────────────── Netz "backend" (intern, kein Internet) ─────────────────────────┐
                  │                                                                                           │
   willhaben.at ◄─┼─ scraper  (scheduler.py)  ──►  db  (PostgreSQL 16)  ◄──  head  (head.py)  ─►  willhaben.at│
                  │   stündlich incremental,        ▲                          Detail-Abrufe + Bilder          │
                  │   wöchentlich full               └── deine Auswertungs-API (liest; legt Aufträge an)       │
                  └───────────────────────────────────────────────────────────────────────────────────────────┘
```

| Container | Aufgabe | Volume |
|---|---|---|
| `db` | PostgreSQL, **kein Port nach außen** | `pgdata` |
| `scraper` | Anzeigen sammeln: stündlich `incremental`, wöchentlich `full` (So 02:00), monatlich Kategorien | `scraper_data` (Locks, Debug) |
| `head` | Der „Kopf“: arbeitet die Tabelle `enrichment_tasks` ab (Detail-API und Bilder) | `media` (Bilder) |

Die Aurena-Daten und deine Auswertungs-API sind nicht enthalten. Die API kann als vierter Container ins selbe
Netz (Beispiel in `docker-compose.yml`, auskommentiert).

## Inbetriebnahme

```bash
bash make-secrets.sh              # erzeugt secrets/*.txt (3 Passwörter, nur einmal nötig)
docker compose up -d --build
docker compose logs -f            # Logs aller Container (Strg+C beendet nur die Anzeige)
```

Beim ersten Start passiert automatisch:
1. Postgres legt die Rollen `willhaben_app` (Scraper/Kopf) und `willhaben_api` (deine API) an.
2. Der Scheduler lädt die Kategorien und startet einen **vollständigen Durchlauf** (über 20 Stunden bei allen Kategorien).
   Der stündliche Incremental-Lauf überspringt Marktplätze, die noch keinen abgeschlossenen Full-Lauf haben.
3. Der Kopf wartet auf Aufträge.

### Erste Prüfung (bitte der Reihe nach)
```bash
docker compose up -d db
docker compose logs db | tail -20                      # "CREATE ROLE" ... keine Fehler?
docker compose run --rm scraper python willhaben_scraper.py categories
docker compose run --rm scraper python willhaben_scraper.py list | head
docker compose run --rm scraper python willhaben_scraper.py splitcheck 4000   # Aufteilung großer Marktplätze
docker compose run --rm head python head.py probe-detail 971448719            # Antwort der Detail-API ansehen
docker compose up -d
```
Den Postgres-Teil und die Docker-Dateien habe ich ohne Docker und ohne Postgres geschrieben und nur die gemeinsame
Logik (über SQLite) getestet. Erwarte beim ersten Start also eventuell Kleinigkeiten.

## Betrieb

```bash
docker compose ps                                   # Status, "healthy" = Heartbeat frisch
docker compose logs -f scraper                      # nur der Scraper
docker compose logs --since 1h head
docker compose restart scraper                      # nach Änderung der config.json
docker compose up -d --build                        # nach Code-Änderungen
docker compose run --rm head python head.py status  # Aufträge nach Art/Status
```

Einstellungen stehen in `config.json` (wird in die Container eingehängt) und lassen sich auch per Umgebungsvariable
`WH_<NAME>` setzen, z. B. `WH_SCHEDULE_FULL='{"weekday":"sat","hour":3,"minute":0}'` im `environment:`-Block.

## Der Kopf: Aufträge anlegen

Eine Zeile in `enrichment_tasks` = ein Auftrag. Art `detail` (Detail-API → JSON in `ad_details`) oder `images`
(Bilder → Dateien in `/media`, Einträge in `ad_image_files`). Kleinere `priority` wird zuerst bearbeitet (Standard 5).

**1. Aus deiner API (Rolle `willhaben_api`, nur INSERT/UPDATE auf diese eine Tabelle):**
```sql
INSERT INTO enrichment_tasks (ad_id, kind, priority, created_at)
VALUES (1234567890, 'detail', 1, now())
ON CONFLICT (ad_id, kind) DO NOTHING;
```
Das Recht dafür vergibt der Kopf beim ersten Start (`ensure_api_grants`); die Anzeige muss in `ads` existieren.

**2. Per Kommandozeile:**
```bash
docker compose run --rm head python head.py enqueue --kind both --ad 1234567890 987654321 --priority 1
docker compose run --rm head python head.py enqueue --kind images --marketplace 4552 --limit 200
```

**3. Automatisch nach Regeln** (`config.json`, Schlüssel `head_auto_rules`), z. B. alle 10 Minuten bis zu 100 neue Aufträge:
```json
"head_auto_rules": [
  {"name": "Fahrräder über 1000 €", "marketplace_ids": [4552], "price_min": 1000,
   "kinds": ["detail", "images"], "limit": 100}
]
```

**Ergebnisse lesen:**
```sql
SELECT data FROM ad_details WHERE ad_id = 1234567890;                      -- JSON (Spalte data)
SELECT position, file_path FROM ad_image_files WHERE ad_id = 1234567890;   -- Pfad relativ zum Medien-Ordner
SELECT status, error FROM enrichment_tasks WHERE ad_id = 1234567890;       -- pending/running/done/gone/failed
```
Die Bilder liegen im Volume `media` unter `<ID mod 1000>/<ID>/<Nr>.jpg`. Deine API hängt das Volume schreibgeschützt ein.

Fehler werden bis zu `head_max_attempts` (5) mit wachsender Wartezeit wiederholt (2, 4, 8 … höchstens 60 Minuten).
HTTP 404/410 → Status `gone` (Anzeige/Bild existiert nicht mehr), keine Wiederholung.

## Proxy (optional, ein einzelner Proxy)

Alle Anfragen an willhaben (Anzeigen-Suche, Detail-API, Bilder) können über **einen** Proxy laufen. Standard: aus.

**Einschalten** in `config.json`, danach `docker compose restart scraper head`:
```json
"proxy_enabled": true,
"proxy_url": "http://proxy.example.com:8080",
"proxy_user": "benutzername",
"proxy_use_for": ["search", "detail", "images"]
```
- **Passwort** nicht in die `config.json`, sondern in die Docker-Secret-Datei: `secrets/proxy_password.txt` (nur das Passwort, ohne Leerzeile
  am Ende nötig, ein Zeilenumbruch wird entfernt). `make-secrets.sh` legt die leere Datei an. Ändern: Datei bearbeiten, dann
  `docker compose up -d` (Secrets werden beim Start eingelesen).
- Alternative: Zugangsdaten direkt in der URL, `"proxy_url": "http://user:passwort@host:8080"` (Sonderzeichen im Passwort als `%40`, `%3A` usw.).
- Erlaubte Schemas: `http`, `https`, `socks5`, `socks5h` (DNS über den Proxy), `socks4`, `socks4a`. SOCKS ist im Image enthalten (`requests[socks]`).
- `proxy_use_for` bestimmt, welche Anfragen über den Proxy gehen. Beispiel: `["search"]` = nur die Anzeigen-Suche, Bilder direkt (spart Proxy-Traffic).

**Ausschalten:** `"proxy_enabled": false` (Proxy-Daten bleiben stehen) und Neustart. Ohne Config-Änderung geht es auch über Umgebungsvariablen
im `environment:`-Block, z. B. `WH_PROXY_ENABLED: "false"`. Für einzelne Aufrufe: `--proxy URL` bzw. `--no-proxy`.

**Testen** (zeigt die öffentliche IP direkt und über den Proxy und ob willhaben erreichbar ist):
```bash
docker compose run --rm scraper python willhaben_scraper.py proxytest
```
Beim Start steht im Log `Verbindung 'search': über Proxy http://user:***@host:8080` (Passwort wird nie geloggt).

**Wichtig:** Die 429-Sperre von willhaben gilt pro IP. Ein Wechsel auf den Proxy bedeutet eine andere IP: Das Tempo-Limit
(`request_delay_seconds`) bleibt trotzdem gültig und die gelernte Pause (`/data/willhaben.rate.json`) bleibt erhalten. Teilen sich mehrere Systeme denselben Proxy,
addieren sich deren Anfragen bei willhaben. Fehler `407` heißt: Proxy lehnt Benutzer/Passwort ab. Der Datenbank-Verkehr geht nie über den Proxy.

## Wichtige Hinweise

- **Tempo und 429-Sperren.** willhaben (Cloudflare) sperrt die IP-Adresse für eine Stunde (`HTTP 429`, `Retry-After`),
  wenn zu viele Anfragen kommen. Die Sperre gilt für alle Anfragen, auch die des stündlichen Incremental-Laufs.
  Der Scraper reagiert so: Alle Läufe im Scraper-Container teilen sich ein Tempo-Limit (`request_delay_seconds`, empfohlen 4).
  Bei einer 429 pausieren alle Läufe bis zum Ende der Sperre (zählt nicht als Fehlversuch), danach verdoppelt sich die Pause
  (höchstens `rate_max_delay_seconds`) und sinkt nach je `rate_recover_after` Anfragen ohne 429 um 20 % bis zum Grundwert zurück.
  Die gelernte Pause steht in `/data/willhaben.rate.json`; zum Zurücksetzen die Datei löschen und den Container neu starten.
  Eine einzelne 429 wird zuerst nur bestätigt: Nach `rate_confirm_delay_seconds` (5 s) kommt eine zweite Anfrage; erst wenn auch sie
  mit 429 antwortet, gilt es als Sperre (abschaltbar mit `"rate_confirm_retries": 0`).
  Der Kopf hat eigene Limiter für Detail-API und Bilder. Beides läuft vom selben Server, die Anfragen addieren sich also bei willhaben.
  Eine Sperre kostet eine Stunde, eine zu hohe Pause nur Zeit: lieber vorsichtig starten.
- **Detail-API ungeprüft.** Die Adresse `head_detail_url_template` stammt aus deinen Beispieldaten. Welche Felder und Header sie
  braucht, zeigt `head.py probe-detail <ID>`.
- **Festplatte.** Die Datenbank wird bei allen Kategorien groß (grob 15–30 GB). Bilder nur für ausgewählte Anzeigen laden
  (Regeln/Aufträge), nicht für alle.
- **Backups.** `docker compose exec db pg_dump -U postgres willhaben | gzip > willhaben_$(date +%F).sql.gz`
  und das Volume `media` getrennt sichern.
- **Secrets.** `secrets/` darf nicht ins Git (steht in `.gitignore`). Der Ordner ist 700, die Dateien sind 444, damit der
  Container-Benutzer sie lesen kann. Die Passwörter ändern sich nicht von selbst: ein späterer Wechsel braucht
  `ALTER ROLE ... PASSWORD ...` in der Datenbank **und** eine neue Secret-Datei.
- **Rollen nachträglich ändern:** Das Init-Skript läuft nur bei leerem `pgdata`. Änderungen an Rollen danach per
  `docker compose exec db psql -U postgres willhaben`.
- **Rechtliches.** Gespeichert werden Adressdaten von Privatverkäufern (DSGVO) und Bilder (Urheberrecht). Prüfe außerdem die
  Nutzungsbedingungen von willhaben.

## Dateien

| Datei | Zweck |
|---|---|
| `willhaben_scraper.py` | Scraper, Datenbank (SQLite/PostgreSQL), Aufteilung großer Marktplätze |
| `scheduler.py` | Zeitplan: incremental, full, Kategorien; Start-Prüfung |
| `head.py` | Der Kopf: Warteschlange, Detail-Abrufe, Bilder |
| `healthcheck.py` | Docker-Healthcheck (Heartbeat-Datei) |
| `config.json` | Einstellungen |
| `Dockerfile`, `requirements.txt` | Ein Image für Scraper und Kopf |
| `docker-compose.yml` | Dienste, Netze, Volumes, Secrets |
| `docker/db-init/10-roles.sh` | Legt beim ersten Start die DB-Rollen an |
| `make-secrets.sh` | Erzeugt die Passwort-Dateien |
