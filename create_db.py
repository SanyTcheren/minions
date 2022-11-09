"""
Create database for projects, use sqlite.
"""

import sys
import sqlite3
import json
import logging
from logging import FileHandler, Formatter
import time
from pathlib import Path


LOGGING_LEVEL = logging.INFO

log_name = f"logs/{int(time.time())}_create_db.log"
logger = logging.getLogger("create_db")
logger.setLevel(LOGGING_LEVEL)
logger_file_handler = FileHandler(log_name)
logger_file_handler.setLevel(LOGGING_LEVEL)
logger_file_handler.setFormatter(Formatter("%(message)s"))
logger.addHandler(logger_file_handler)

connection = sqlite3.connect('minions.sqlite')
cursor = connection.cursor()
cursor.executescript("""
DROP TABLE IF EXISTS Profession;
DROP TABLE IF EXISTS Worker;
DROP TABLE IF EXISTS Category;
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
    talon INT NOT NULL,
    name TEXT NOT NULL,
    birthday TEXT,
    passport TEXT,
    sign BLOB,
    profession_id INT NOT NULL,
    category_id INT
);
                     """)

# Worker give from workers.json, sign from ./Signs/ by talon-number
try:
    with open('workers.json', encoding="utf-8") as fdata:
        jdata = json.loads(fdata.read())
except FileNotFoundError as exc:
    log_dict = {"ts": time.time(), "message": "didn't found worker.json"}
    logger.error(json.dumps(log_dict))
    print(exc)
    sys.exit()

for row in jdata:
    cursor.execute("INSERT OR IGNORE INTO Profession (name) VALUES (?)", (row[2],))
    cursor.execute("SELECT id FROM Profession WHERE name = ? ", (row[2],))
    profession_id = cursor.fetchone()[0]

    cursor.execute("INSERT OR IGNORE INTO Category (name) VALUES (?)", (row[3],))
    cursor.execute("SELECT id FROM Category WHERE name = ? ", (row[3],))
    category_id = cursor.fetchone()[0]

    sign_path = Path('signs', str(row[0])+'.png')
    SIGN_BLOB = None
    try:
        with open(sign_path, mode="rb") as sign_file:
            SIGN_BLOB = sqlite3.Binary(sign_file.read())
    except FileNotFoundError:
        log_dict = {"ts": time.time(), "message": f"not find sign for {row[0]}"}
        logger.warning(json.dumps(log_dict))
        print(log_dict["message"])
        continue

    passport = None if row[5].startswith('YYYY') else row[5]

    cursor.execute("""INSERT OR IGNORE INTO Worker
                   (talon, name, birthday, passport, sign, profession_id, category_id)
                   VALUES (?, ?, ? ,?, ?, ?, ?)""",
        (int(row[0]), row[1], row[4], passport, SIGN_BLOB, profession_id, category_id))

    log_dict = {"ts": time.time(), "message": f"add {row[0]} in table Worker"}
    logger.info(json.dumps(log_dict))

    connection.commit()
