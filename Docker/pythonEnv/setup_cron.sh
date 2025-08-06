#!/bin/bash

# === USAGE ===
# This script sets up a cron job to run a given script on schedule.
# ./setup_cron.sh /full/path/to/script.sh "0 1 * * *"
# ./setup_cron.sh /full/path/to/script.sh "@reboot"

# === INPUT ===
SCRIPT_PATH="$1"
SCHEDULE="$2"

# === VALIDATION ===
if [ -z "$SCRIPT_PATH" ] || [ -z "$SCHEDULE" ]; then
  echo "Usage: $0 /full/path/to/script.sh \"<cron schedule or @reboot>\""
  exit 1
fi

if [ ! -f "$SCRIPT_PATH" ]; then
  echo "Error: Script not found at $SCRIPT_PATH"
  exit 1
fi

# === MAKE SCRIPT EXECUTABLE ===
chmod +x "$SCRIPT_PATH"
echo "Made script executable: $SCRIPT_PATH"

# === BUILD CRON ENTRY ===
CRON_JOB="$SCHEDULE $SCRIPT_PATH"

# === INSTALL OR REPLACE CRON JOB ===
( crontab -l 2>/dev/null | grep -vF "$SCRIPT_PATH" ; echo "$CRON_JOB" ) | crontab -

echo -e "\nCron job installed/replaced:\n$CRON_JOB"
echo -e "\nCurrent crontab jobs:"
crontab -l
