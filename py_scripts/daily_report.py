# -*- coding: utf8 -*-

from . import project_constants as const
from . import table_utils

def init_view_report_day_transacts(cursor, report_date):
	cursor.execute(f'''
	CREATE VIEW if not exists V_REPORT_DAY_TRANSACTIONS as
		SELECT {const.TRANS_TABLE_COLUMNS}
		FROM DWH_FACT_TRANSACTIONS
		WHERE date('{report_date}') = date(transaction_date);
	''')

def get_fraud_transactions_bad_passport(cursor, report_date):
	cursor.execute(
	f'''
	CREATE VIEW if not exists V_BAD_PASSPORT_TRANSACTIONS AS 
		SELECT
			t1.*,
			t2.account,
			t3.card_num,
			t4.*
		FROM (
				SELECT 
					* 
				FROM clients
				WHERE (passport_valid_to IS NOT NULL AND date(passport_valid_to) <= date('{report_date}'))
					OR passport_num IN (SELECT passport_num FROM DWH_FACT_PASSPORT_BLACKLIST)
			) t1
		INNER JOIN accounts t2 ON t1.client_id = t2.client
		INNER JOIN cards t3 ON t2.account = t3.account
		INNER JOIN V_REPORT_DAY_TRANSACTIONS t4 ON t3.card_num = t4.card_num
	;
	''')
	cursor.execute(
	f'''
	INSERT INTO REP_FRAUD(
		event_dt,
		passport,
		fio,
		phone,
		event_type,
		report_dt
	)
	SELECT
		transaction_date,
		passport_num,
		first_name || ' ' || patronymic || ' ' || last_name,
		phone,
		'Совершение операции при просроченном или заблокированном паспорте',
		date('{report_date}')
	FROM V_BAD_PASSPORT_TRANSACTIONS
	;
	''')
	cursor.execute('drop view if exists V_BAD_PASSPORT_TRANSACTIONS;')

def get_fraud_transactions_bad_account(cursor, report_date):
	cursor.execute(
	f'''
	CREATE VIEW if not exists V_BAD_ACCOUNT_TRANSACTIONS AS 
		SELECT
			t1.*,
			t2.account,
			t3.card_num,
			t4.*
		FROM (
				SELECT 
					* 
				FROM accounts
				WHERE date(valid_to) <= date('{report_date}')
			) t2
		INNER JOIN clients t1 ON t1.client_id = t2.client
		INNER JOIN cards t3 ON t2.account = t3.account
		INNER JOIN V_REPORT_DAY_TRANSACTIONS t4 ON t3.card_num = t4.card_num
	;
	''')
	cursor.execute(
	f'''
	INSERT INTO REP_FRAUD(
		event_dt,
		passport,
		fio,
		phone,
		event_type,
		report_dt
	)
	SELECT
		transaction_date,
		passport_num,
		first_name || ' ' || patronymic || ' ' || last_name,
		phone,
		'Совершение операции при недействующем договоре',
		date('{report_date}')
	FROM V_BAD_ACCOUNT_TRANSACTIONS
	;
	''')
	cursor.execute('drop view if exists V_BAD_ACCOUNT_TRANSACTIONS;')

def get_fraud_transactions_diff_cities(cursor, report_date):
	cursor.execute(
	f'''
	CREATE VIEW if not exists STG_PIVOT_VIEW AS
		SELECT
			t4.client_id,
			t4.first_name,
			t4.patronymic,
			t4.last_name,
			t4.phone,
			t1.transaction_date,
			t5.terminal_city
		FROM V_REPORT_DAY_TRANSACTIONS t1
		INNER JOIN cards t2 ON t1.card_num = t2.card_num
		INNER JOIN accounts t3 ON t2.account = t3.account
		INNER JOIN clients t4 ON t3.client = t4.client_id
		INNER JOIN (
			SELECT
				*
			FROM DWH_DIM_TERMINALS_HIST
			WHERE current_timestamp BETWEEN effective_from AND effective_to
		) t5 ON t1.terminal = t5.terminal_id
	;
	''')
	cursor.execute('''
	CREATE VIEW if not exists tmp AS
		SELECT
			t1.*,
			t2.transaction_date as second_transaction_date,
			t2.terminal_city as second_terminal_city
		FROM STG_PIVOT_VIEW t1 
		INNER JOIN STG_PIVOT_VIEW t2
		ON (
			t1.client_id = t2.client_id 
			and t1.terminal_city <> t2.terminal_city 
			and (ABS(julianday(t1.transaction_date) * 24 - julianday(t2.transaction_date) * 24) <= 1)
		)
		GROUP BY t2.client_id, t2.terminal_city, t2.transaction_date
	''')
	cursor.execute('''
		CREATE VIEW if not exists tmp1 AS
	''')
	table_utils.showTable(cursor, 'tmp')
	cursor.execute('drop view if exists STG_PIVOT_VIEW;')
	cursor.execute('drop view if exists tmp;')
	cursor.execute('drop view if exists tmp1;')

def get_daily_report(con, report_date):
	init_view_report_day_transacts(con.cursor(), report_date)
	get_fraud_transactions_bad_passport(con.cursor(), report_date)
	get_fraud_transactions_bad_account(con.cursor(), report_date)
	get_fraud_transactions_diff_cities(con.cursor(), report_date)
	con.cursor().execute('drop view if exists V_REPORT_DAY_TRANSACTIONS;')
	con.commit()