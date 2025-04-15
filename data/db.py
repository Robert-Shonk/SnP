import sqlite3

with open('schema.sql', 'r') as file:
    script = file.read()

db = sqlite3.connect('snp_db.sqlite')
cursor = db.cursor()
cursor.executescript(script)
db.commit()
db.close()