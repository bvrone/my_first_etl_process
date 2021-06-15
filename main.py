import sqlite3

_initSchemaScriptName = 'ddl_dml.sql'

def initSchema():
	conn = sqlite3.connect('BANK.db')
	cursor = conn.cursor()
	try :
		with open(_initSchemaScriptName, 'r') as f:
			data = f.read()
		data_json = json.loads(data)
		for obj in data_json:
			addClientInfo(conn, obj)
	except FileNotFoundError as e:
		print("Could not find a file with test data", "'" + filename + "'")