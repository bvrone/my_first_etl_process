# -*- coding: utf8 -*-
import sqlite3
from datetime import datetime
from datetime import timedelta

from py_scripts import table_utils
from py_scripts import data_upload_utils
from py_scripts import project_constants as const
from py_scripts import init_tables
from py_scripts import daily_report as report

con = sqlite3.connect(const.DB_NAME)
cursor = con.cursor()
print('-' * 50)
init_tables.init_schema(con)
init_tables.init_uploaded_data_tables(con)
init_tables.init_report_table(con)
upload_date = const.UPLOAD_DATE_START

for _ in range(const.REPORT_DATES_COUNT):
	upload_date_str = datetime.strftime(upload_date, "%d%m%Y")
	data_upload_utils.upload_daily_data(upload_date_str, con)
	report_date = datetime.strftime(upload_date, "%Y-%m-%d")
	report.get_daily_report(con, report_date)
	upload_date += timedelta(days=1)
	print('-' * 50)

con.commit()
con.close()