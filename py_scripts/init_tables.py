# -*- coding: utf8 -*-
import sqlite3

from . import project_constants as const

def init_schema(con):
	print('Creating schema...')
	try :
		with open(const.INIT_SCHEMA_SCRIPT_NAME, 'r') as f:
			schema = f.read()
	except FileNotFoundError as e:
		print("Could not find a test data file", "'" + const.INIT_SCHEMA_SCRIPT_NAME + "'")
		exit(1)
	try :
		con.executescript(schema)
	except sqlite3.OperationalError as e:
		print("Schema already exists")
		return
	con.commit()
	print("Done")

def init_table_transactions(cursor):
	query = """
	--sql
	CREATE TABLE if not exists DWH_FACT_TRANSACTIONS(
		transaction_id varchar(128) primary key,
		transaction_date date,
		amount decimal,
		card_num varchar(128),
		oper_type varchar(128),
		oper_result varchar(128),
		terminal varchar(128)
	);
	"""
	cursor.execute(query)

def init_table_passport_blacklist(cursor):
	query = """
	--sql
	CREATE TABLE if not exists DWH_FACT_PASSPORT_BLACKLIST(
		passport_num varchar(128) UNIQUE ON CONFLICT REPLACE,
		entry_dt date
	);
	"""
	cursor.execute(query)

def init_table_terminals(cursor):
	query = """
	--sql
	CREATE TABLE if not exists DWH_DIM_TERMINALS_HIST(
		terminal_id varchar(128),
		terminal_type varchar(128),
		terminal_city varchar(128),
		terminal_address varchar(128),
		deleted_flg integer default 0,
		effective_from datetime default current_timestamp,
		effective_to datetime default (datetime('2999-12-31 23:59:59'))
	);
	"""
	cursor.execute(query)
	cursor.execute(f'''
	CREATE VIEW if not exists V_TERMINALS as
		SELECT {const.TERMS_TABLE_BUSINESS_COLUMNS}
		FROM DWH_DIM_TERMINALS_HIST
		WHERE current_timestamp between effective_from and effective_to;
	''')

def init_uploaded_data_tables(con):
	init_table_transactions(con.cursor())
	init_table_passport_blacklist(con.cursor())
	init_table_terminals(con.cursor())
	con.commit()

def init_report_table(con):
	query = """
	--sql
	CREATE TABLE if not exists REP_FRAUD(
		--id integer primary key autoincrement,
		event_dt date,
		passport varchar(128),
		fio varchar(384),
		phone varchar(128),
		event_type varchar(384),
		report_dt date
	);
	"""
	con.cursor().execute(query)
	con.commit()
