# Data Portfolio
## ETL Pipeline by Purna Shah
### Introduction
This project is an ETL pipeline written in Python that takes raw data in the form of a single CSV (meant to represent a recurring data source) and batch extracts it to multiple CSVs in an AWS S3 bucket. Then, the CSVs in the buckets are transformed and loaded to an AWS RDS PostgreSQL instance.
### Choice of technologies
In my previous position at 2U, we mostly followed the ELT paradigm. Typical pipelines I worked on used Airflow to extract and load to a Snowflake warehouse and DBT to transform. So, I wanted to try the other common paradigm for this project and build a traditional ETL pipeline. I wrote the pipeline in Python; PySpark and Airflow are good options as well but they don't have an unlimited free option. I wanted to use a production-level warehouse for faster upserts but now that Redshift is off the AWS Free Tier, I chose PostgreSQL for my RDS instance. PostgreSQL is slower than Redshift up to a factor of 1000, so to improve performance on my personal computer I limited the raw CSV to 20000 rows and optimized the code. However, this pipeline design could accomodate high volumes of data with a production-level warehouse.
### Data Model
For raw data, I use a car sales dataset from Kaggle, and transform it into one fact table (car sales) and one dimension table (car details) i.e. a star schema with one "point." A car can be sold multiple times and the dataset does not contain a natural key, so a composite key is made from the VIN and the sale date. The two tables can be joined on their primary keys.
### Dimension Types
If a row theoretically were to be changed in the raw car data, then it would be a correction to the record and the old record should be discarded. So, I implemented "type 1 slowly changing dimensions," or a traditional upsertion load into the DB.
### Installation Steps
- Create a new environment in your environment manager and run `pip install -r /path/to/requirements.txt`.
- Set the environmental variables referenced in variables_functions.py.
- Run using `python main.py`. This project is tested in Python 3.10.