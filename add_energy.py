"""Add energy of meter into database from file.
Use first argument for file name or nothing for default powerprofile.txt."""


import sys
import json
import sqlite3
import logging
from logging import FileHandler, Formatter
import time
from datetime import datetime, timedelta


LOGGING_LEVEL = logging.INFO

log_name = f"logs/{int(time.time())}_add_energy.log"
logger = logging.getLogger("add_energy")
logger.setLevel(LOGGING_LEVEL)
logger_file_handler = FileHandler(log_name)
logger_file_handler.setLevel(LOGGING_LEVEL)
logger_file_handler.setFormatter(Formatter("%(message)s"))
logger.addHandler(logger_file_handler)

# Определение файла с данными
if len(sys.argv) > 1:
    data_file = sys.argv[1]
else:
    data_file = "powerprofile.txt"
log_dict = {
    "ts": time.time(),
    "message": f"read energy data from {data_file}",
}
logger.info(json.dumps(log_dict))


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
cursor.execute("SELECT number FROM Meter WHERE id = ?", (meter_id,))
meter_number = cursor.fetchone()[0]

count = 0
with open(data_file, encoding="cp1251") as fd:
    for index, line in enumerate(fd.readlines()):
        if index == 1:
            # Проверка соответствия номера счетчка в базе данных и в файле с данными
            infile_number = line.split()[2]
            if meter_number != infile_number:
                log_dict = {
                    "ts": time.time(),
                    "message": (
                        f"mismatch of numbers in the file({infile_number})"
                        + f" and in the database({meter_number})"
                    ),
                }
                logger.warning(json.dumps(log_dict))
                print("[WARNING]", log_dict["message"])
                input("Continue?")
        elif index > 4:
            component = line.split()
            day, month, year = component[0].split(".")
            hh_mm0, hh_mm1 = component[1].split("-")
            h0, m0 = hh_mm0.split(":")
            h1, m1 = hh_mm1.split(":")
            start = datetime(int(year), int(month), int(day), int(h0), int(m0), 0)
            # Обработка частного случая, когда в отчете используется 24 час.
            if h1 == "24":
                h1 = "00"
                delta = timedelta(days=1)
            else:
                delta = timedelta(days=0)
            end = datetime(int(year), int(month), int(day), int(h1), int(m1), 0) + delta
            try:
                active = float(component[2].replace(",", "."))
            except (ValueError, IndexError):
                active = 0
            try:
                reactive = float(component[4].replace(",", "."))
            except (ValueError, IndexError):
                reactive = 0

            cursor.execute(
                """INSERT OR REPLACE INTO Energy
                (meter_id, start, end, active, reactive) VALUES (?, ?, ?, ?, ?)""",
                (meter_id, start, end, active, reactive),
            )
            count += 1
            if count % 10 == 0:
                connection.commit()

connection.commit()
connection.close()
log_dict = {"ts": time.time(), "message": f"add or replace {count} row in Energy"}
logger.info(json.dumps(log_dict))
print(log_dict["message"])
