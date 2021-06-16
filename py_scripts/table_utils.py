# -*- coding: utf8 -*- 
import sqlite3
import pandas as pd

def showTable(cursor, tableName):
	cursor.execute(f'select * from {tableName};')
	print('-_'*10 + "Table " + tableName + '_-'*10)
	for row in cursor.fetchall():
		print(row)

def csv2sql(filePath, tableName, con):
	df = pd.read_csv(filePath)
	df.to_sql(tableName, con=con, if_exists='replace')

def exel2sql(filePath, tableName, con):
	df = pd.read_excel(filePath)
	df.to_sql(name=tableName, con=con, if_exists='replace')