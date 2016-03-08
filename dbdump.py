import sqlite3 as lite
import codecs

con = lite.connect("files.db")
dbdump = codecs.open("dbdump.txt", "a", "utf-8")

with con:
	cur = con.cursor()

	query = "SELECT filename, done, copied FROM File"
	cur.execute(query)
	result = cur.fetchall()

	for filename, done, copied in result:
		dbdump.write("File: %s, Done: %s, Copied: %s \n" % (filename, done, copied))