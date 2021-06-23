# -*- coding: utf8 -*- 
import pandas as pd

def showTable(cursor, tableName):
	cursor.execute(f'select * from {tableName};')
	print('-_'*10 + tableName + '_-'*10)
	print([i[0] for i in cursor.description])
	for row in cursor.fetchall():
		print(row)

def csv2sql(filePath, tableName, con, sep):
	try:
		df = pd.read_csv(filePath, sep=sep)
		df.to_sql(tableName, con=con, if_exists='replace')
	except FileNotFoundError as e:
		print("Could not find a test data file", "'" + filePath + "'")
		exit(1)

def excel2sql(filePath, tableName, con):
	try:
		df = pd.read_excel(filePath)
		df.to_sql(name=tableName, con=con, if_exists='replace')
	except FileNotFoundError as e:
		print("Could not find a test data file", "'" + filePath + "'")
		exit(1)