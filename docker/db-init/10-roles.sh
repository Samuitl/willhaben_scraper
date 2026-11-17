#!/bin/bash
# Wird von der Postgres-Imagevorlage EINMALIG beim ersten Start (leeres Datenverzeichnis) ausgeführt.
# Legt zwei Rollen an – die Passwörter kommen aus Docker Secrets, nicht aus Umgebungsvariablen:
#   willhaben_app  Besitzer der Datenbank; Scraper, Scheduler und Kopf nutzen diese Rolle
#   willhaben_api  für die Auswertungs-API: liest alle Tabellen (Default-Rechte unten);
#                  Aufträge anlegen darf sie, sobald der Kopf einmal gestartet ist (ensure_api_grants)
set -euo pipefail

APP_PW="$(< /run/secrets/db_app_password)"
API_PW="$(< /run/secrets/db_api_password)"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v app_pw="$APP_PW" -v api_pw="$API_PW" -v dbname="$POSTGRES_DB" <<'EOSQL'
CREATE ROLE willhaben_app LOGIN PASSWORD :'app_pw';
CREATE ROLE willhaben_api LOGIN PASSWORD :'api_pw';

ALTER DATABASE :"dbname" OWNER TO willhaben_app;
REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO willhaben_api;

ALTER SCHEMA public OWNER TO willhaben_app;
GRANT USAGE ON SCHEMA public TO willhaben_api;

-- Tabellen, die willhaben_app später anlegt, darf willhaben_api lesen
ALTER DEFAULT PRIVILEGES FOR ROLE willhaben_app IN SCHEMA public
    GRANT SELECT ON TABLES TO willhaben_api;
EOSQL
