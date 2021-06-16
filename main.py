import sqlite3
import pandas as pd

from py_scripts import table_utils

_initSchemaScriptName = './sql_scripts/ddl_dml.sql'

def initSchema():
	conn = sqlite3.connect('./data/BANK.db')
	try :
		print('Creating schema...')
		with open(_initSchemaScriptName, 'r') as f:
			schema = f.read()
		conn.executescript(schema)
		print("Done")
	except FileNotFoundError as e:
		print("Could not find a file with test data", "'" + _initSchemaScriptName + "'")
	conn.commit()
	conn.close()



#initSchema()
conn = sqlite3.connect('./data/BANK.db')
cursor = conn.cursor()
df = pd.read_excel('./data/passport_blacklist_01032021.xlsx')
df.to_sql(name='passport_blacklist_tmp', con=conn, if_exists='replace')
table_utils.showTable(cursor, 'passport_blacklist_tmp')
conn.close()