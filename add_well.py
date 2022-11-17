"Add well into database."

import sys
import json
import sqlite3
import logging
from logging import FileHandler, Formatter
import time
from datetime import datetime

LOGGING_LEVEL = logging.INFO

log_name = f"logs/{int(time.time())}_add_well.log"
logger = logging.getLogger("add_well")
logger.setLevel(LOGGING_LEVEL)
logger_file_handler = FileHandler(log_name)
logger_file_handler.setLevel(LOGGING_LEVEL)
logger_file_handler.setFormatter(Formatter("%(message)s"))
logger.addHandler(logger_file_handler)


connection = sqlite3.connect("minions.sqlite")
cursor = connection.cursor()

brigade_number = input("Enter brigade number: ")
cursor.execute("SELECT id FROM Brigade WHERE number = ?", (brigade_number.strip(),))
result = cursor.fetchone()
if result is None:
    log_dict = {
        "ts": time.time(),
        "message": f"didn't found brigade number {brigade_number} in DB.",
    }
    logger.error(json.dumps(log_dict))
    print("ERROR:", log_dict["message"])
    sys.exit()
brigade_id = result[0]

while True:
    well_number = input("Enter well number:").strip()
    if well_number != "":
        break
    else:
        print("It's not ISO 8601, try again")

while True:
    start = input("Enter start time for well (use ISO 8601):")
    try:
        start_date = datetime.fromisoformat(start)
        break
    except ValueError:
        print("It's not ISO 8601, try again")

while True:
    end = input("Enter end time for well (use ISO 8601 or skip:")
    if end.strip() == "":
        end_date = None
        break
    try:
        end_date = datetime.fromisoformat(end)
        break
    except ValueError:
        print("It's not ISO 8601, try again")

if end_date is not None:
    cursor.execute(
        """INSERT OR REPLACE INTO Well
        (brigade_id, number, start, end) VALUES (?, ?, ?, ?)""",
        (brigade_id, well_number, start_date, end_date),
    )
else:
    cursor.execute(
        "INSERT OR REPLACE INTO Well (brigade_id, number, start) VALUES (?, ?, ?)",
        (brigade_id, well_number, start_date),
    )
connection.commit()
connection.close()
