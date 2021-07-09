# -*- coding: utf8 -*-

import csv

from . import project_constants as const
from . import table_utils

# Инициализация представления таблицы транзакций срезом по дню составления отчета
def init_view_report_day_transacts(cursor, report_date):
	cursor.execute(f'''
	CREATE VIEW if not exists V_REPORT_DAY_TRANSACTIONS as
		SELECT {const.TRANS_TABLE_COLUMNS}
		FROM DWH_FACT_TRANSACTIONS
		WHERE date('{report_date}') = date(transaction_date);
	''')

# Формируются строки с отчетом по первому типу мошеннических операций - 
# Совершение операции при просроченном или заблокированном паспорте.
# Результат загружается в таблицу - отчет REP_FRAUD.
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


# Формируются строки с отчетом по первому типу мошеннических операций - 
# Совершение операции при недействующем договоре.
# Результат загружается в таблицу - отчет REP_FRAUD.
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


# Третий тип фрауд операций:
# Совершение операций в разных городах в течение одного часа.
def get_fraud_transactions_diff_cities(cursor, report_date):
	# Запрос создает объединенное view всех таблиц для получения всех необходимых полей
	query = """
	--sql
	CREATE VIEW if not exists STG_PIVOT_VIEW AS
		SELECT
			t4.client_id,
			t4.first_name,
			t4.patronymic,
			t4.last_name,
			t4.phone,
			t4.passport_num,
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
	"""
	cursor.execute(query)
	# Запрос, объединяющий все пары операций одного клиента из разных городов в течении часа.
	# Cчитаю каждую пару отдельной мошеннической операцией,
	# они будут загружены в таблицу - отчет c большей датой в качестве event_dt.
	query = """
	--sql
	CREATE VIEW if not exists V_DIFF_CITIES_FROAD_TRANS AS
		SELECT
			t1.client_id,
			t1.first_name,
			t1.patronymic,
			t1.last_name,
			t1.phone,
			t1.passport_num,
			t1.transaction_date,
			t2.transaction_date as second_transaction_date,
			t1.terminal_city,
			t2.terminal_city as second_terminal_city
		FROM STG_PIVOT_VIEW t1 
		INNER JOIN STG_PIVOT_VIEW t2
		ON (
			t1.client_id = t2.client_id 
			and t1.terminal_city <> t2.terminal_city 
			and (ABS(julianday(t1.transaction_date) * 24 - julianday(t2.transaction_date) * 24) <= 1)
		)
		GROUP BY t2.client_id, t2.terminal_city, t2.transaction_date
	;
	"""
	cursor.execute(query)
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
		CASE
			when transaction_date >= second_transaction_date
				then transaction_date
				else second_transaction_date
		END,
		passport_num,
		first_name || ' ' || patronymic || ' ' || last_name,
		phone,
		'Совершение операций в разных городах в течение одного часа',
		date('{report_date}')
	FROM V_DIFF_CITIES_FROAD_TRANS
	;
	''')
	# Еще один запрос, демонстрирующий подход,
	# когда можно считать мошенническими только операции, возникающие во втором городе.
	query = """
	--sql
	CREATE VIEW if not exists V2_DIFF_CITIES_FROAD_TRANS AS
		SELECT
			*
		FROM (
			SELECT
				client_id,
				first_name,
				patronymic,
				last_name,
				phone,
				transaction_date,
				LEAD(transaction_date) OVER w AS lead_date,
				terminal_city,
				LEAD(terminal_city) OVER w AS lead_city
			FROM STG_PIVOT_VIEW
			WINDOW w AS (PARTITION BY client_id ORDER BY transaction_date)
		)
		WHERE terminal_city <> lead_city
		AND ABS(julianday(lead_date) - julianday(transaction_date)) * 24 <= 1
	;
	"""
	cursor.execute(query)
	cursor.execute('drop view if exists STG_PIVOT_VIEW;')
	cursor.execute('drop view if exists V_DIFF_CITIES_FROAD_TRANS;')
	cursor.execute('drop view if exists V2_DIFF_CITIES_FROAD_TRANS;')

# Четвертый тип мошеннических операций:
# Попытка подбора суммы. 
# В течение 20 минут проходит более 3х операций со следующим шаблоном – 
# каждая последующая меньше предыдущей, при этом отклонены все кроме последней. 
# Последняя операция (успешная) в такой цепочке считается мошеннической.
def get_fraud_transactions_amount_select(cursor, report_date):
	query = """
	--sql
	CREATE VIEW if not exists V_BAD_AMT_SELECTION_TRANS AS
		SELECT
			*
		FROM (
			SELECT
				LAG(amount) OVER w AS lag_amount,
				amount,
				LEAD(amount) OVER w AS lead_amount,
				LAG(oper_result) OVER w AS lag_oper_result,
				oper_result,
				LEAD(oper_result) OVER w AS lead_oper_result,
				LAG(transaction_date) OVER w AS lag_trans_date,
				transaction_date,
				LEAD(transaction_date) OVER w AS lead_trans_date,
				LEAD(card_num) OVER w AS lead_card_num
			FROM V_REPORT_DAY_TRANSACTIONS
			WINDOW w AS (ORDER BY transaction_date)
		)
		WHERE (julianday(lead_trans_date) - julianday(lag_trans_date)) * 24 * 60 <= 20
		and lag_amount > amount and amount > lead_amount
		and lag_oper_result = 'REJECT' and oper_result = 'REJECT'
		and lead_oper_result = 'SUCCESS'
	;
	"""
	cursor.execute(query)
	query = """
	--sql
	CREATE VIEW if not exists V_BAD_AMT_SELECTION_CLIENTS_DATA AS
		SELECT
			t4.first_name,
			t4.patronymic,
			t4.last_name,
			t4.phone,
			t4.passport_num,
			t1.lead_trans_date as transaction_date,
			t1.lead_card_num
		FROM V_BAD_AMT_SELECTION_TRANS t1
		INNER JOIN cards t2 ON t1.lead_card_num = t2.card_num
		INNER JOIN accounts t3 ON t2.account = t3.account
		INNER JOIN clients t4 ON t3.client = t4.client_id
	;
	"""
	cursor.execute(query)
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
		'Попытка подбора суммы',
		date('{report_date}')
	FROM V_BAD_AMT_SELECTION_CLIENTS_DATA
	;
	''')
	cursor.execute('drop view if exists V_BAD_AMT_SELECTION_TRANS;')
	cursor.execute('drop view if exists V_BAD_AMT_SELECTION_CLIENTS_DATA;')

def save_report_to_csv(cursor, report_date):
	cursor.execute('SELECT * FROM REP_FRAUD;')
	res = cursor.fetchall()
	report_filepath = const.REPORT_DIR + 'report_' + report_date + '.csv'
	print('Saving a new report to', report_filepath)
	with open(report_filepath, "w", newline='') as csv_file:
		csv_writer = csv.writer(csv_file)
		csv_writer.writerow([i[0] for i in cursor.description])
		csv_writer.writerows(res)
	print("Done")

def get_daily_report(con, report_date):
	print('Adding a new report data to the table...')
	init_view_report_day_transacts(con.cursor(), report_date)
	get_fraud_transactions_bad_passport(con.cursor(), report_date)
	get_fraud_transactions_bad_account(con.cursor(), report_date)
	get_fraud_transactions_diff_cities(con.cursor(), report_date)
	get_fraud_transactions_amount_select(con.cursor(), report_date)
	con.cursor().execute('drop view if exists V_REPORT_DAY_TRANSACTIONS;')
	con.commit()
	save_report_to_csv(con.cursor(), report_date)