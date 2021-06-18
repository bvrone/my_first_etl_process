import os

from . import table_utils
from . import project_constants as const

def update_table_transactions(cursor):
	cursor.execute(f'''
		INSERT INTO DWH_FACT_TRANSACTIONS(
			{const.TRANS_TABLE_COLUMNS}
		)select
			{const.TRANS_TABLE_COLUMNS}
		from STG_TRANSACTIONS
	''')

def update_table_passport_blacklist(cursor):
	query = """
	--sql
	INSERT INTO DWH_FACT_PASSPORT_BLACKLIST(
		passport_num,
		entry_dt
	)SELECT
		passport,
		date
	FROM STG_PASSPORT_BLACKLIST;
	"""
	cursor.execute(query)

def create_table_terms_new_rows(cursor):
	query = """
	--sql
	CREATE TABLE STG_TERMINALS_NEW_ROWS as
		SELECT t1.*
		FROM STG_TERMINALS t1
		LEFT JOIN V_TERMINALS t2
		ON t1.terminal_id = t2.terminal_id
		WHERE t2.terminal_id IS NULL;
	"""
	cursor.execute(query)

def create_table_terms_del_rows(cursor):
	query = """
	--sql
	CREATE TABLE STG_TERMINALS_DEL_ROWS as
		SELECT t1.terminal_id
		FROM V_TERMINALS t1 
		LEFT JOIN STG_TERMINALS t2
		ON t1.terminal_id = t2.terminal_id
		WHERE t2.terminal_id IS NULL;
	"""
	cursor.execute(query)

def create_table_terms_changed_rows(cursor):
	query = """
	--sql
	CREATE TABLE STG_TERMINALS_CHANGED_ROWS as
		SELECT t1.*
		FROM STG_TERMINALS t1
		INNER JOIN V_TERMINALS t2
		ON t1.terminal_id = t2.terminal_id
		AND (
			t1.terminal_type <> t2.terminal_type
			or t1.terminal_city <> t2.terminal_city
			or t1.terminal_address <> t2.terminal_address
		)
	;
	"""
	cursor.execute(query)

def create_terms_tmp_tables(cursor):
	create_table_terms_new_rows(cursor)
	create_table_terms_del_rows(cursor)
	create_table_terms_changed_rows(cursor)

def del_terms_tmp_tables(cursor):
	cursor.execute('drop table if exists STG_TERMINALS_NEW_ROWS;')
	cursor.execute('drop table if exists STG_TERMINALS_DEL_ROWS;')
	cursor.execute('drop table if exists STG_TERMINALS_CHANGED_ROWS;')

def update_table_terminals(cursor):
	create_terms_tmp_tables(cursor)

	query = """
	--sql
		UPDATE DWH_DIM_TERMINALS_HIST
		SET	
			effective_to = datetime('now', '-1 second'),
			deleted_flg = 1
		WHERE terminal_id IN (SELECT terminal_id FROM STG_TERMINALS_DEL_ROWS)
		and effective_to = datetime('2999-12-31 23:59:59')
	;
	"""
	cursor.execute(query)

	query = """
	--sql
		UPDATE DWH_DIM_TERMINALS_HIST
		SET effective_to = datetime('now', '-1 second')
		WHERE terminal_id IN (SELECT terminal_id FROM STG_TERMINALS_CHANGED_ROWS)
		and effective_to = datetime('2999-12-31 23:59:59')
	;
	"""
	cursor.execute(query)
	
	cursor.execute(f'''
		INSERT INTO DWH_DIM_TERMINALS_HIST(
			{const.TERMS_TABLE_BUSINESS_COLUMNS}
		)select
			{const.TERMS_TABLE_BUSINESS_COLUMNS}
		from STG_TERMINALS_NEW_ROWS
	''')

	cursor.execute(f'''
		INSERT INTO DWH_DIM_TERMINALS_HIST(
			{const.TERMS_TABLE_BUSINESS_COLUMNS}
		)select
			{const.TERMS_TABLE_BUSINESS_COLUMNS}
		from STG_TERMINALS_CHANGED_ROWS
	''')

	del_terms_tmp_tables(cursor)

def upload_stg_table(entity_name, file_ext, upload_date, con):
	file_name = entity_name + '_' + upload_date + file_ext
	file_path = const.DATA_DIR + file_name
	table_name = 'STG_' + entity_name.upper()
	if file_ext == '.xlsx':
		table_utils.excel2sql(file_path, table_name, con)
	else:
		table_utils.csv2sql(file_path, table_name, con, ';')
	archive_file_path = const.ARCHIVE_DIR + file_name + '.backup'
	#os.replace(file_path, archive_file_path)

def upload_daily_data(upload_date, con):
	upload_stg_table('passport_blacklist', '.xlsx', upload_date, con)
	upload_stg_table('terminals', '.xlsx', upload_date, con)
	upload_stg_table('transactions', '.txt', upload_date, con)
	update_table_transactions(con.cursor())
	update_table_passport_blacklist(con.cursor())
	update_table_terminals(con.cursor())
	con.commit()