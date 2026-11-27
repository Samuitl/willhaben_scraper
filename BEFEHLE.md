# Befehle für Betrieb und Fehlersuche

Alle Befehle im Projektordner ausführen (dort, wo `docker-compose.yml` liegt).
`psql`-Abfragen laufen im Container `db`; Abkürzung für alle SQL-Befehle:

```bash
alias wsql='docker compose exec -T db psql -U postgres willhaben -c'
# Beispiel: wsql "SELECT COUNT(*) FROM ads;"
```

---

## 1. Wie weit ist der Scraper?

**Läuft alles?**
```bash
docker compose ps                                   # db, scraper, head: Up / healthy
docker compose logs scraper --tail 30               # letzte Zeilen
docker compose logs -f scraper                      # live mitlesen (Strg+C beendet nur die Anzeige)
```

**Fortschritt des Full-Laufs** (Zähler `[n/m]` = Marktplatz n von m im aktuellen Lauf):
```bash
docker compose logs scraper | grep "(Modus: full)" | tail -3
docker compose logs scraper --since 30m | grep -E "Seite [0-9]+:" | tail -3      # letzte gescrapte Seiten
```

**Marktplätze: wie viele fertig?**
```bash
wsql "SELECT COUNT(*) AS gesamt, COUNT(last_scraped_at) AS schon_gescrapt,
             COUNT(last_full_scrape_at) AS full_fertig,
             COUNT(*) - COUNT(last_full_scrape_at) AS full_offen FROM marketplaces;"
```

**Noch offene Marktplätze** (größte zuerst):
```bash
wsql "SELECT m.id, c.name AS hauptkategorie, m.name, m.ad_count
      FROM marketplaces m JOIN main_categories c ON c.id = m.main_category_id
      WHERE m.last_full_scrape_at IS NULL ORDER BY m.ad_count DESC NULLS LAST LIMIT 30;"
```

**Fortschritt je Hauptkategorie:**
```bash
wsql "SELECT c.name AS hauptkategorie, COUNT(m.last_full_scrape_at) AS full_fertig, COUNT(*) AS marktplaetze
      FROM marketplaces m JOIN main_categories c ON c.id = m.main_category_id
      GROUP BY c.name ORDER BY full_fertig DESC;"
```

**Anzeigen in der Datenbank:**
```bash
wsql "SELECT COUNT(*) AS anzeigen, COUNT(DISTINCT marketplace_id) AS marktplaetze_mit_anzeigen,
             SUM(is_active) AS aktiv FROM ads;"
wsql "SELECT date_trunc('hour', first_seen_at) AS stunde, COUNT(*) AS neue_anzeigen
      FROM ads WHERE first_seen_at > now() - interval '24 hours' GROUP BY 1 ORDER BY 1;"
```

**Letzte Läufe (Full / Incremental):**
```bash
wsql "SELECT r.id, r.mode, m.name, r.started_at, r.finished_at, r.pages, r.ads_seen, r.ads_new, r.status, r.stop_reason
      FROM scrape_runs r JOIN marketplaces m ON m.id = r.marketplace_id
      ORDER BY r.started_at DESC LIMIT 15;"
wsql "SELECT r.id, m.name, r.started_at, r.pages, r.ads_seen, r.status, r.stop_reason
      FROM scrape_runs r JOIN marketplaces m ON m.id = r.marketplace_id
      WHERE r.mode = 'full' ORDER BY r.started_at DESC LIMIT 10;"
```
Ein Eintrag mit `status = running` und altem `started_at` ist ein Lauf, der abgebrochen wurde (z. B. durch Neustart). Das ist nur kosmetisch.

**Größe der Datenbank:**
```bash
wsql "SELECT pg_size_pretty(pg_database_size('willhaben')) AS datenbank;"
wsql "SELECT relname AS tabelle, pg_size_pretty(pg_total_relation_size(relid)) AS groesse
      FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 10;"
df -h .                                              # freier Platz auf dem Server
```

---

## 2. Jobs und Scheduler

```bash
docker compose logs scraper | grep -E "Job '" | tail -20                     # Start/Ende der Jobs
docker compose logs scraper | grep -E "Fehlgeschlagen|abgebrochen|Abbruch|Traceback|aufgegeben" | tail -20
docker compose logs scraper --since 12h | grep "\[startup\]" | tail -25     # Start-Lauf (Full beim Start)
docker compose logs scraper | grep -E "Fortsetzen" | tail -5                # automatisch fortgesetzte Full-Läufe
docker compose logs scraper | grep -B15 "full (Start)' beendet"             # Warum endete der Start-Lauf?
```
- Der Full-Lauf startet beim Container-Start (nur offene Marktplätze), danach prüft `full-resume` alle 30 Minuten, ob noch etwas offen ist. Sonntags um 02:00 läuft ein Full-Lauf über alle.
- `Job 'full-resume' beendet (Exit-Code 0)` direkt nach dem Start heißt nur: Es läuft schon ein Full-Lauf, nichts zu tun.

---

## 3. 429-Sperren (Tempo)

```bash
docker compose logs scraper --since 24h | grep -c "HTTP 429"                                 # alle 429
docker compose logs scraper --since 24h | grep -c "Bestätigungs-Anfrage [0-9]/"              # Wiederholungen
docker compose logs scraper --since 24h | grep -c "Bestätigungs-Anfrage erfolgreich"         # Einzelfälle ohne Sperre
docker compose logs scraper --since 24h | grep -c "Sperre [0-9]/"                            # echte Sperren
docker compose logs scraper --since 24h | grep -E "HTTP 429|Tempo-Limit" | tail -20
docker compose logs scraper --since 24h | grep "HTTP 429" | awk '{print $3, substr($4,1,2)":00"}' | sort | uniq -c   # je Stunde
```
- Gelernte Pause zurücksetzen (z. B. nach einem Proxy-Wechsel):
  ```bash
  docker compose exec scraper rm -f /data/willhaben.rate.json
  docker compose restart scraper
  ```
- Tempo ändern: `request_delay_seconds` in `config.json`, dann `docker compose restart scraper head`
  (`up -d` allein startet bei geänderter Config-Datei nicht neu).

---

## 4. Proxy

```bash
docker compose logs scraper | grep "Verbindung"                              # nutzt der Scraper den Proxy?
docker compose logs scraper --since 3h | grep -E "Proxy nicht erreichbar|Proxy wieder"
docker compose run --rm scraper python willhaben_scraper.py proxytest        # IP direkt/über Proxy, willhaben erreichbar?
```

**Test mit den Kopfzeilen des Scraper** (aussagekräftiger als curl; 6943 = Comics/Mangas):
```bash
for i in 1 2 3 4 5 6 7 8; do docker compose exec -T scraper python willhaben_scraper.py probe 6943 2>&1 | grep -E "rowsFound|FEHLER|HTTP|Proxy|Fehler" | head -2; echo "---"; done
```
`rowsFound` = durchgekommen, `HTTP 403` = Ausgang abgelehnt, `Proxy nicht erreichbar` = Tunnel gescheitert.

**Tunnel direkt am Rotator testen** (echte Zugangsdaten statt BENUTZER:PASSWORT einsetzen, `407` heißt: Zugangsdaten falsch/fehlen):
```bash
for i in $(seq 1 10); do curl -sS -m 20 -o /dev/null -w "%{http_code}\n" -x http://127.0.0.1:8080 -U "BENUTZER:PASSWORT" https://www.willhaben.at/ 2>&1 | tail -1; done
```
`301` = normale Weiterleitung, `403` = von Cloudflare abgelehnt (bei curl ohne Browser-Kopfzeilen nicht aussagekräftig), `000`/curl-Fehler = Tunnel gescheitert.

**Hängt der Scraper im Proxy-Netzwerk, und stimmt die Verbindung?**
```bash
docker compose config --format json | python3 -c "import json,sys; d=json.load(sys.stdin); [print(k, list(v.get('networks',{}))) for k,v in d['services'].items()]"
# erwartet: db ['backend'] / head und scraper ['backend','internet','proxy_rotator_default']
docker compose exec scraper python -c "import socket; print(socket.gethostbyname('proxy-rotator'))"
docker compose exec -T scraper python - <<'EOF'
import socket, struct
for l in open('/proc/net/tcp').read().splitlines()[1:]:
    p = l.split()
    if p[3] == '01':
        ip, port = p[2].split(':')
        print(socket.inet_ntoa(struct.pack('<I', int(ip, 16))), int(port, 16))
EOF
# erwartet: Verbindung zum Proxy (…:8080) und zur Datenbank (…:5432), keine direkte Verbindung auf Port 443
```

---

## 5. Container, Neustart, Update

```bash
docker compose restart scraper head          # nach Änderung der config.json
docker compose up -d --build                 # nach Code-Änderungen / neuer Version
docker compose logs db --tail 30             # Datenbank-Meldungen ("checkpoint complete" ist normal)
docker compose logs head --tail 30           # Kopf (Detail-Abrufe, Bilder)
docker compose run --rm head python head.py status     # Aufträge nach Art/Status
docker stats --no-stream                     # CPU/Speicher der Container
docker volume ls                             # willhaben_pgdata, willhaben_scraper_data, willhaben_media
docker system df                             # Platz von Docker
```

**Neue Version holen (Git):**
```bash
git status
git stash push -m "meine Einstellungen" -- config.json docker-compose.yml    # nur nötig, wenn du dort etwas geändert hast
git pull
git stash pop
docker compose config --quiet && echo "Compose gültig"
docker compose up -d --build
```

**Backup und Rückspielen:**
```bash
docker compose exec -T db pg_dump -U postgres willhaben | gzip > willhaben_backup_$(date +%F).sql.gz
ls -lh willhaben_backup_*.sql.gz
# Rückspielen NUR in eine leere Datenbank:
# gunzip -c willhaben_backup_DATUM.sql.gz | docker compose exec -T db psql -U postgres willhaben
```

> **Niemals** ausführen: `docker compose down -v`, `docker volume rm …`, `docker system prune --volumes`
> (löschen die Datenbank). Normales `docker compose down` ist sicher.
> Und: `rm -r` im Projektordner löscht Compose-Datei, Code und `secrets/` (die Daten liegen im Volume und bleiben erhalten).

---

## 6. Problem → mögliche Ursache → Befehl

| Beobachtung | Mögliche Ursache | Was prüfen |
|---|---|---|
| Nur Incremental läuft, Full nicht | Start-Lauf abgebrochen | `grep -B15 "full (Start)' beendet"`, danach wartet `full-resume` (alle 30 Min) |
| `HTTP 429 (Sperre n/8)` | IP-Sperre von willhaben, Pause bis `Retry-After` | Abschnitt 3; nicht neu starten, das hilft nicht |
| `Proxy nicht erreichbar (ProxyError/SSLError)` | Rotator-Pool leer oder Proxy ohne HTTPS | Abschnitt 4; im Rotator nur socks4/socks5 und HTTPS-fähige Proxys |
| `HTTP 407` | Proxy-Zugangsdaten falsch | `proxy_user` in `config.json`, Passwort in `secrets/proxy_password.txt`, dann `docker compose up -d` |
| `HTTP 403` | IP/Ausgang von Cloudflare abgelehnt | `probe`-Test (Abschnitt 4), anderen Proxy-Pool testen |
| `NameResolutionError` bei `proxy-rotator` | Scraper nicht im Netzwerk `proxy_rotator_default` | Netzwerk-Prüfung (Abschnitt 4), `docker compose up -d` |
| `psycopg … the connection is closed / AdminShutdown` | Datenbank wurde neu gestartet | harmlos, nächster Lauf baut neu auf |
| Container `Restarting` | Fehler beim Start (z. B. Syntaxfehler im Skript) | `docker compose logs scraper --tail 50` |
| `no space left on device` | Platte voll | `df -h`, `docker system df`, alte Backups löschen |
| Kein Lauf, Log still | Heartbeat/Pause, 429-Wartezeit | `docker compose logs scraper --tail 5`, Uhrzeit im letzten `Sperre`-Eintrag |
| `permission denied` bei `secrets/…` | Datei hat Rechte 444 oder gehört `root` | `chmod 644 …`, schreiben, `chmod 444 …` |
