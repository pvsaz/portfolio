# library imports
import psycopg2
import pandas as pd

# file imports
from variables_functions import *

conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)
cursor = conn.cursor()
df = pd.read_csv('car_prices.csv')

def test_all_records_loaded_and_upsert():
    # There are 19999 rows in the raw csv. Because we are upserting records and there are some duplicates on the PK in the raw data,
    # there should be fewer rows in the final Postgres table.

    # get row counts
    cursor.execute(sql.SQL("select count(*) from cardetails"))
    cardetailscount = cursor.fetchone()[0]
    cursor.execute(sql.SQL("select count(*) from carsales"))
    carsalescount = cursor.fetchone()[0]

    # find number of duplicates in raw data to prove final table has correct row count
    df['pk'] = df['vin'] + df['saledate']
    duplicate = df[df.duplicated('pk')]
    dupes = len(duplicate.index)

    assert cardetailscount == 19999 - dupes and carsalescount == 19999 - dupes

def test_column_names():
    # Test that both tables have been assigned the correct column names.
    cursor.execute('''SELECT *
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name   = 'cardetails'
     ;''')
    details_raw = list(cursor.fetchall())
    details_column_list = list()
    for tuple in details_raw:
        details_column_list.append(tuple[3])
    cursor.execute('''SELECT *
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name   = 'carsales'
     ;''')
    sales_raw = list(cursor.fetchall())
    sales_column_list = list()
    for tuple in sales_raw:
        sales_column_list.append(tuple[3])
    assert details_column_list == list(cardetailscolumnlist) and sales_column_list == list(carsalescolumnlist)

def test_transformations():
    # check that a raw row had its empty strings correctly converted to NULLs in the final DB
    cursor.execute('select * from cardetails where vin = \'1g1pc5sb6e7128803\'')
    row_final_nulls = cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    row_final_nulls_dict = dict()
    for i, val in enumerate(row_final_nulls):
        row_final_nulls_dict[columns[i]] = val
    
    cursor.execute('select * from cardetails where vin = \'2g1fa1e39e9134494\'')
    row_final_body = cursor.fetchone()
    row_final_body_dict = dict()
    for i, val in enumerate(row_final_body):
        row_final_body_dict[columns[i]] = val
    cursor.close()
    assert row_final_nulls_dict['condition'] == None and row_final_nulls_dict['transmission'] == None and row_final_body_dict['body'] == '2-door coupe'
