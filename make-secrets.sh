#!/usr/bin/env bash
# Erzeugt die drei Passwort-Dateien für Docker Secrets (bestehende werden nicht überschrieben).
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p secrets
chmod 700 secrets      # Ordner nur für dich lesbar; die Dateien selbst sind 444, damit der Container-Benutzer sie lesen kann

for name in postgres_admin_password db_app_password db_api_password; do
  file="secrets/${name}.txt"
  if [ -e "$file" ]; then
    echo "$file existiert bereits - übersprungen"
    continue
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24 | tr -d '\n' > "$file"
  else
    python3 -c 'import secrets; print(secrets.token_hex(24), end="")' > "$file"
  fi
  chmod 444 "$file"
  echo "$file erstellt"
done

# Proxy-Passwort: leere Datei als Platzhalter (Passwort bei Bedarf eintragen, siehe README, Abschnitt Proxy)
if [ ! -e secrets/proxy_password.txt ]; then
  : > secrets/proxy_password.txt
  chmod 444 secrets/proxy_password.txt
  echo "secrets/proxy_password.txt erstellt (leer)"
fi
