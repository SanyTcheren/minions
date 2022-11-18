"Add value of meter into database."

import sys
import json
import sqlite3
import logging
from logging import FileHandler, Formatter
import time
from datetime import datetime

LOGGING_LEVEL = logging.INFO

log_name = f"logs/{int(time.time())}_add_value.log"
logger = logging.getLogger("add_value")
logger.setLevel(LOGGING_LEVEL)
logger_file_handler = FileHandler(log_name)
logger_file_handler.setLevel(LOGGING_LEVEL)
logger_file_handler.setFormatter(Formatter("%(message)s"))
logger.addHandler(logger_file_handler)


connection = sqlite3.connect("minions.sqlite")
cursor = connection.cursor()

brigade_number = input("Enter brigade number: ")
cursor.execute(
    "SELECT id, meter_id FROM Brigade WHERE number = ?", (brigade_number.strip(),)
)
result = cursor.fetchone()
if result is None:
    log_dict = {
        "ts": time.time(),
        "message": f"didn't found brigade number {brigade_number} in DB.",
    }
    logger.error(json.dumps(log_dict))
    print("ERROR:", log_dict["message"])
    sys.exit()
brigade_id, meter_id = result

while True:
    day_inp = input("Enter time for value (use ISO 8601):")
    try:
        day = datetime.fromisoformat(day_inp)
        break
    except ValueError:
        print("It's not ISO 8601, try again")

while True:
    P = input("Enter ACTIVE power (float number):")
    try:
        active = float(P)
        break
    except ValueError:
        print("It's not float number, try again")

while True:
    Q = input("Enter REACTIVE power (float number):")
    try:
        reactive = float(Q)
        break
    except ValueError:
        print("It's not float number, try again")

cursor.execute(
    """INSERT OR REPLACE INTO Value
    (meter_id, day, active, reactive) VALUES (?, ?, ?, ?)""",
    (meter_id, day, active, reactive),
)

connection.commit()
connection.close()
