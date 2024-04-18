# library imports
import pandas as pd, boto3, psycopg2, threading
from io import StringIO
from psycopg2 import sql

# file imports
from variables_functions import *

### EXTRACTION ###

object_key = 0

# Instantiate boto3 client
s3 = boto3.client(
    "s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key
)

# use pandas instead of native csv read method in order to use chunk processing (load incremental chunks of raw CSV)
# to staging bucket as individual CSVs
with pd.read_csv("car_prices.csv", chunksize=1000, keep_default_na=False) as reader:
    for chunk in reader:
        # Upload the data to the S3 bucket
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{object_key}.csv",
            Body=chunk.to_csv(header=True, index=False),
            ContentType="text/csv",
        )
        print(f"Chunk uploaded to s3://{bucket_name}/{object_key}.csv")
        object_key += 1

# Typically, we stage raw data during extraction rather than extracting, holding in memory, then loading right to the DB
# for a few reasons. Separating the processes cleanly (extract to S3, then transform and load from S3) prevents malformed data
# from being loaded if there is an issue with the raw data source (eg an API, not a flat file like here.)  Also, failures
# are generally easier to pinpoint and diagnose. Here, we are staging the data as we would if we were building a traditional
# ETL pipeline, and the single flat file is a dummy representation of a recurring data source for the purposes of demonstration.

# Set up Postgres connection and create the destination tables if needed. We'll divide the raw data into one fact table
# (car sales) and one dimension table (car details), i.e. a star schema with one "point."
conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)

cursor = conn.cursor()

# Because this pipeline loads a flat file, it makes sense to drop the tables every
# time the pipeline is run. The flat file is a stand-in for a recurring data source,
# and if we were using a recurring data source instead and inserting values from it,
# then we would not want to drop the tables every time and instead insert to existing
# tables.
cursor.execute(sql.SQL("drop table if exists cardetails"))
cursor.execute(sql.SQL("drop table if exists carsales"))
conn.commit()

# Create our tables if they don't exist already
cursor.execute(cardetailsqry)
cursor.execute(carsalesqry)
conn.commit()

# Show that currently, nothing has been loaded to the DB
checktablecounts(cursor)

# Set up loop to iterate through the CSV files in the bucket
for num in range(0, object_key):
    csv_obj = s3.get_object(Bucket=bucket_name, Key=f"{num}.csv")
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_string))

    ### TRANSFORMATIONS AND LOADING ###

    # We will convert body type "Coupe" to "2-door coupe."
    df["body"] = df["body"].apply(lambda x: "2-door coupe" if x == "Coupe" else x)

    # Combining VIN and sale datetime to create a composite primary key for insertion/update
    df["primarykey"] = df["saledate"].astype(str) + df["vin"]

    # If a row theoretically were to be changed in the car data, then it would be a correction to the record and the old record
    # should be discarded. So, we will implement "type 1 slowly changing dimensions," or an insertion of new rows with a replacement
    # of updated rows into the DB. I am iterating through the dataframe with the itertuples method instead of iterrows, in addition 
    # to using multithreading, for a speed advantage.

    for row in df.itertuples():
        cardetailsthread = threading.Thread(
            target=generate_run_query,
            args=(
                cardetailscolumndict["tablename"],
                cardetailscolumnlist,
                [row._asdict()[i] for i in cardetailscolumnlist],
                cardetailscolumnlistnopk,
                excardetailscolumnlistnopk,
                cursor,
            ),
        )
        carsalesthread = threading.Thread(
            target=generate_run_query,
            args=(
                carsalescolumndict["tablename"],
                carsalescolumnlist,
                [row._asdict()[i] for i in carsalescolumnlist],
                carsalescolumnlistnopk,
                excarsalescolumnlistnopk,
                cursor,
            ),
        )
        cardetailsthread.start()
        carsalesthread.start()
        cardetailsthread.join()
        carsalesthread.join()
        conn.commit()

    print(f"{num}.csv transformed and loaded to database!")

checktablecounts(cursor)
print("All done, closing connection.")
conn.close()
