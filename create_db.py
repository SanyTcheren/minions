"""
Create database for brigade , use sqlite.
"""

import sys
import sqlite3
import json
import logging
from logging import FileHandler, Formatter
import time
from pathlib import Path
import openpyxl
from datetime import date


LOGGING_LEVEL = logging.INFO

log_name = f"logs/{int(time.time())}_create_db.log"
logger = logging.getLogger("create_db")
logger.setLevel(LOGGING_LEVEL)
logger_file_handler = FileHandler(log_name)
logger_file_handler.setLevel(LOGGING_LEVEL)
logger_file_handler.setFormatter(Formatter("%(message)s"))
logger.addHandler(logger_file_handler)


connection = sqlite3.connect("minions.sqlite")
cursor = connection.cursor()
cursor.executescript(
    """
DROP TABLE IF EXISTS Field;
DROP TABLE IF EXISTS Place;
DROP TABLE IF EXISTS Rig;
DROP TABLE IF EXISTS Brigade;
DROP TABLE IF EXISTS Dps;
DROP TABLE IF EXISTS Runtime;
DROP TABLE IF EXISTS Profession;
DROP TABLE IF EXISTS Worker;
DROP TABLE IF EXISTS Category;
DROP TABLE IF EXISTS Location;
DROP TABLE IF EXISTS Works;
DROP TABLE IF EXISTS Maintenance;
CREATE TABLE Field (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE,
    customer TEXT
);
CREATE TABLE Place (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    field_id INT NOT NULL,
    bush TEXT NOT NULL
);
CREATE TABLE Rig (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    type TEXT NOT NULL,
    number TEXT NOT NULL
);
CREATE TABLE Brigade (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    number TEXT UNIQUE,
    place_id INT,
    rig_id INT
);
CREATE TABLE Dps (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    brigade_id INT,
    garage_number TEXT NOT NULL UNIQUE,
    factory_number TEXT NOT NULL UNIQUE,
    manufacturer TEXT NOT NULL,
    type TEXT NOT NULL,
    year TEXT NOT NULL,
    power INT NOT NULL,
    voltage INT NOT NULL,
    current INT,
    purpose TEXT,
    tank INT,
    consumption INT
);
CREATE TABLE Runtime (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    dps_id INT NOT NULL,
    date TEXT NOT NULL,
    general INT NOT NULL,
    daily INT,
    notes TEXT
);
CREATE TABLE Profession (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE
);
CREATE TABLE Category (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE
);
CREATE TABLE Worker (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    talon INT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    birthday TEXT,
    passport TEXT,
    sign BLOB,
    profession_id INT NOT NULL,
    category_id INT,
    brigade_id INT NOT NULL
);
CREATE TABLE Location (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE
);
CREATE TABLE Works (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    list TEXT UNIQUE
);
CREATE TABLE Maintenance (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    hardware TEXT UNIQUE,
    location_id INT NOT NULL,
    works_id INT NOT NULL,
    brigade_id INT NOT NULL
);
                     """
)

# Worker give from workers.json, sign from ./Signs/ by talon-number
try:
    with open("workers.json", encoding="utf-8") as fdata:
        jdata = json.loads(fdata.read())
except FileNotFoundError as exc:
    log_dict = {"ts": time.time(), "message": "didn't found worker.json"}
    logger.error(json.dumps(log_dict))
    print(exc)
    sys.exit()

# Maintenance give from monthly.xlsx
try:
    monthly_wb = openpyxl.load_workbook("monthly.xlsx")
except FileNotFoundError as exc:
    log_dict = {"ts": time.time(), "message": "didn't found monthly.xlsx"}
    logger.error(json.dumps(log_dict))
    print(exc)
    sys.exit()

# Brigade give from brigade.json
try:
    with open("brigade.json", encoding="utf-8") as fbrigade:
        brigade_data = json.loads(fbrigade.read())
except FileNotFoundError as exc:
    log_dict = {"ts": time.time(), "message": "didn't found brigade.json"}
    logger.error(json.dumps(log_dict))
    print(exc)
    sys.exit()

# add brigade data into database
field_name = brigade_data[0]["place"]["field"]
customer = brigade_data[0]["place"]["customer"]
cursor.execute(
    "INSERT OR REPLACE INTO Field (name, customer) VALUES (?, ?)",
    (field_name, customer),
)
cursor.execute("SELECT id FROM Field WHERE name = ?", (field_name,))
field_id = cursor.fetchone()[0]

bush = brigade_data[0]["place"]["bush"]
cursor.execute(
    "INSERT OR REPLACE INTO Place (field_id, bush) VALUES (?, ?)",
    (field_id, bush),
)
cursor.execute(
    "SELECT id FROM Place WHERE field_id = ? AND bush = ?",
    (field_id, bush),
)
place_id = cursor.fetchone()[0]

rig_type = brigade_data[0]["rig"]["type"]
rig_number = brigade_data[0]["rig"]["number"]
cursor.execute(
    "INSERT OR REPLACE INTO Rig (type, number) VALUES (?, ?)",
    (rig_type, rig_number),
)
cursor.execute(
    "SELECT id FROM Rig WHERE type = ? AND number = ?",
    (rig_type, rig_number),
)
rig_id = cursor.fetchone()[0]

brigade_number = brigade_data[0]["number"]
cursor.execute(
    "INSERT OR REPLACE INTO Brigade (number, place_id, rig_id) VALUES (?, ?, ?)",
    (brigade_number, place_id, rig_id),
)
cursor.execute(
    "SELECT id FROM Brigade WHERE number = ?",
    (brigade_number,),
)
brigade_id = cursor.fetchone()[0]

garage_number = brigade_data[0]["dps"][0]["garage_number"]
factory_number = brigade_data[0]["dps"][0]["factory_number"]
manufacturer = brigade_data[0]["dps"][0]["manufacturer"]
type_dps = brigade_data[0]["dps"][0]["type"]
year = brigade_data[0]["dps"][0]["year"]
power = brigade_data[0]["dps"][0]["power"]
voltage = brigade_data[0]["dps"][0]["voltage"]
current = brigade_data[0]["dps"][0]["current"]
purpose = brigade_data[0]["dps"][0]["purpose"]
tank = brigade_data[0]["dps"][0]["tank"]
consumption = brigade_data[0]["dps"][0]["consumption"]
cursor.execute(
    """
    INSERT OR REPLACE INTO  Dps (garage_number, factory_number, manufacturer,
    type, year, power, voltage, current, purpose, tank, consumption, brigade_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        garage_number,
        factory_number,
        manufacturer,
        type_dps,
        year,
        power,
        voltage,
        current,
        purpose,
        tank,
        consumption,
        brigade_id,
    ),
)
cursor.execute("SELECT id FROM Dps WHERE garage_number = ?", (garage_number,))
dps_id = cursor.fetchone()[0]

today = date.today()
general = brigade_data[0]["dps"][0]["general"]
cursor.execute(
    """INSERT OR REPLACE INTO Runtime (dps_id, date, general, daily, notes)
    VALUES (?, ?, ?, ?, ?)""",
    (dps_id, today, general, 0, ""),
)

connection.commit()

# add maintenance into database
monthly_sheet = monthly_wb.active
ROW_READ = 4
while True:
    location = monthly_sheet[f"B{ROW_READ}"].value
    maintenance = monthly_sheet[f"C{ROW_READ}"].value
    works = monthly_sheet[f"D{ROW_READ}"].value
    if location is None:
        break
    ROW_READ += 3

    cursor.execute("INSERT OR IGNORE INTO Location (name) VALUES (?)", (location,))
    cursor.execute("SELECT id FROM Location WHERE name = ? ", (location,))
    location_id = cursor.fetchone()[0]

    cursor.execute("INSERT OR IGNORE INTO Works (list) VALUES (?)", (works,))
    cursor.execute("SELECT id FROM Works WHERE list = ? ", (works,))
    works_id = cursor.fetchone()[0]

    cursor.execute(
        """INSERT OR IGNORE INTO Maintenance
                   (hardware, location_id, works_id, brigade_id) VALUES (?, ?, ?, ?)""",
        (maintenance, location_id, works_id, brigade_id),
    )

    connection.commit()

    log_dict = {"ts": time.time(), "message": f"add {maintenance} in table Maintenance"}
    logger.info(json.dumps(log_dict))

# add workers into database
for row in jdata:
    cursor.execute("INSERT OR IGNORE INTO Profession (name) VALUES (?)", (row[2],))
    cursor.execute("SELECT id FROM Profession WHERE name = ? ", (row[2],))
    profession_id = cursor.fetchone()[0]

    cursor.execute("INSERT OR IGNORE INTO Category (name) VALUES (?)", (row[3],))
    cursor.execute("SELECT id FROM Category WHERE name = ? ", (row[3],))
    category_id = cursor.fetchone()[0]

    sign_path = Path("signs", str(row[0]) + ".png")
    SIGN_BLOB = None
    try:
        with open(sign_path, mode="rb") as sign_file:
            SIGN_BLOB = sqlite3.Binary(sign_file.read())
    except FileNotFoundError:
        log_dict = {"ts": time.time(), "message": f"not find sign for {row[0]}"}
        logger.warning(json.dumps(log_dict))
        print(log_dict["message"])
        continue

    passport = None if row[5].startswith("YYYY") else row[5]

    cursor.execute(
        """INSERT OR IGNORE INTO Worker
           (talon, name, birthday, passport, sign,
            profession_id, category_id, brigade_id)
           VALUES (?, ?, ? ,?, ?, ?, ?, ?)""",
        (
            int(row[0]),
            row[1],
            row[4],
            passport,
            SIGN_BLOB,
            profession_id,
            category_id,
            brigade_id,
        ),
    )

    connection.commit()

    log_dict = {"ts": time.time(), "message": f"add {row[0]} in table Worker"}
    logger.info(json.dumps(log_dict))
