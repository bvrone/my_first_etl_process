# -*- coding: utf8 -*- 

from datetime import datetime

DATA_DIR = './data/'
ARCHIVE_DIR = './archive/'
REPORT_DIR = './reports/'
TRANS_TABLE_COLUMNS = '''
	transaction_id,
	transaction_date,
	amount,
	card_num,
	oper_type,
	oper_result,
	terminal
'''
TERMS_TABLE_BUSINESS_COLUMNS = '''
	terminal_id,
	terminal_type,
	terminal_city,
	terminal_address
'''
INIT_SCHEMA_SCRIPT_NAME = './sql_scripts/ddl_dml.sql'
DB_NAME = './data/BANK.db'
UPLOAD_DATE_START = datetime.strptime( "01-03-2021", "%d-%m-%Y")
REPORT_DATES_COUNT = 3