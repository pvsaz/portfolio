from psycopg2 import sql
import math, os

### FUNCTIONS ###


def generate_run_query(
    tablename, columnlist, valuelist, columnlistwithoutpk, excluded_prepend, cursor
):
    # Empty strings are used in the raw data to denote NULL, including in integer columns, and are converted
    # to NaN during reading the CSV. The NaNs must be converted to NULL (unquoted) to correctly denote NULL
    # in Postgres. To fix this, we check each value in the results before they are interpolated into the query string.
    # If the value is NaN, we interpolate it into the query string as an unquoted NULL.
    values_with_nulls = []
    for val in valuelist:
        if type(val) == float and math.isnan(val):
            values_with_nulls.append("NULL")
        if type(val) in [int, float] and not math.isnan(val):
            values_with_nulls.append(str(val))
        elif type(val) == str:
            values_with_nulls.append("'{}'".format(val.replace("'", "''")))
    insert_sql = f"""
    INSERT INTO {tablename} ({', '.join(columnlist)})
    VALUES ({', '.join(values_with_nulls)})
    ON CONFLICT (primarykey) DO UPDATE SET
    ({', '.join(columnlistwithoutpk)}) = ({', '.join(excluded_prepend)});
"""
    cursor.execute(insert_sql)


def checktablecounts(cursor):
    cursor.execute(sql.SQL("select count(*) from cardetails"))
    print(str(cursor.fetchone()[0]) + " rows in cardetails table!")
    cursor.execute(sql.SQL("select count(*) from carsales"))
    print(str(cursor.fetchone()[0]) + " rows in carsales table!")


### ENVIRONMENTAL VARIABLES ###

bucket_name = os.environ['bucket_name']
aws_access_key = os.environ['aws_access_key']
aws_secret_access_key = os.environ['aws_secret_access_key']
database = os.environ['database']
user = os.environ['user']
password = os.environ['password']
host = os.environ['host']
port = os.environ['port']

### COMMON VARIABLES ####

cardetailscolumndict = {
    "tablename": "cardetails",
    "pk": "primarykey",
    0: "year",
    1: "make",
    2: "model",
    3: "trim",
    4: "body",
    5: "transmission",
    6: "vin",
    7: "state",
    8: "condition",
    9: "odometer",
    10: "color",
    11: "interior",
}

carsalescolumndict = {
    "tablename": "carsales",
    "pk": "primarykey",
    0: "vin",
    1: "seller",
    2: "mmr",
    3: "sellingprice",
    4: "saledate",
}

cardetailsqry = "create table if not exists {} ({} VARCHAR(255) UNIQUE, {} INT, {} VARCHAR(255), {} VARCHAR(255), {} VARCHAR(255), {} VARCHAR(255), {} VARCHAR(255), {} VARCHAR(255), {} VARCHAR(255), {} INT, {} INT, {} VARCHAR(255), {} VARCHAR(255))".format(
    *cardetailscolumndict.values(), cardetailscolumndict["pk"]
)

cardetailsqry = sql.SQL(cardetailsqry)

carsalesqry = "create table if not exists {} ({} VARCHAR(255) UNIQUE, {} VARCHAR(255), {} VARCHAR(255), {} INT, {} INT, {} VARCHAR(255))".format(
    *carsalescolumndict.values(), carsalescolumndict["pk"]
)

carsalesqry = sql.SQL(carsalesqry)

cardetailscolumnlist = {
    i: cardetailscolumndict[i] for i in cardetailscolumndict if i != "tablename"
}.values()

carsalescolumnlist = {
    i: carsalescolumndict[i] for i in carsalescolumndict if i != "tablename"
}.values()

cardetailscolumnlistnopk = {
    i: cardetailscolumndict[i]
    for i in cardetailscolumndict
    if i not in ["tablename", "pk"]
}.values()

carsalescolumnlistnopk = {
    i: carsalescolumndict[i] for i in carsalescolumndict if i not in ["tablename", "pk"]
}.values()

excardetailscolumnlistnopk = ["EXCLUDED." + x for x in cardetailscolumnlistnopk]

excarsalescolumnlistnopk = ["EXCLUDED." + x for x in carsalescolumnlistnopk]
