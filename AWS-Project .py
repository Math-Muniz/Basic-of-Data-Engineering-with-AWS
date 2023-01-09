# %%
#Imports
import boto3
import pandas as pd
from io import StringIO
import time
import redshift_connector

# %%
#Variables needed
AWS_ACCESS_KEY = "AKIATTZG3B5QV5AN37ZW"
AWS_SECRET_KEY = "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"
AWS_REGION = "sa-east-1"
SCHEMA_NAME = "covid_19"
S3_STAGING_DIR = "s3://math-test-bucket/output/"
S3_BUCKET_NAME = "math-test-bucket"
S3_OUTPUT_DIRECTORY = "output"

# %%
#Login to Athena Client
athena_client = boto3.client(
    "athena",
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# %%
#I create a function to download and load the query results 
Dict = {}
def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            #This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId = query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yeat finished" in str(err):
                time.sleep(1)
            else:
                raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name = AWS_REGION,
        )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
)
    return pd.read_csv(temp_file_location)

# %%
#Execute the query in Athena Client after that i can use the function created above
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM enigma_jhud;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
response

# %%
enigma_jhud = download_and_load_query_results(athena_client, response)

# %%
enigma_jhud.head()

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM nytimes_data_in_usa_us_county;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
nytimes_data_in_usa_us_county = download_and_load_query_results(athena_client, response)

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM nytimes_data_in_usa_us_states;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
nytimes_data_in_usa_us_state = download_and_load_query_results(athena_client, response)

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM static_datacountrycode;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
static_datacountrycode = download_and_load_query_results(athena_client, response)

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM static_datacountypopulation;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
static_datacountypopulation = download_and_load_query_results(athena_client, response)

# %%
static_datacountypopulation.head()

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_dataus_daily;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
rearc_covid_19_testing_dataus_daily = download_and_load_query_results(athena_client, response)

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_usa_hospital_beds;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
rearc_usa_hospital_beds = download_and_load_query_results(athena_client, response)

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_dataus_total_latest;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
rearc_covid_19_testing_dataus_total_latest = download_and_load_query_results(athena_client, response)

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_states_dailystates_daily;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
rearc_covid_19_testing_data_states_dailystates_daily = download_and_load_query_results(athena_client, response)

# %%
rearc_covid_19_testing_data_states_dailystates_daily.head()

# %%
#Execute the query in Athena Client to execute the function download_and_load_query_results
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM static_datastate_abv;",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }, 
)

# %%
static_datastate_abv = download_and_load_query_results(athena_client, response)

# %%
static_datastate_abv.head()

# %%
new_header = static_datastate_abv.iloc[0] #grab the first row for the header
static_datastate_abv = static_datastate_abv[1:] #take the data less the header row
static_datastate_abv.columns = new_header #set the header row as the df header

# %%
#New DataFrame after change the header
static_datastate_abv.head()

# %%
#Create a Table factCovid using enigma_jhud and rearc_covid_19_testing_data_states_dailystates_daily
factCovid_1 = enigma_jhud[['fips', 'province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active']]
factCovid_2 = rearc_covid_19_testing_data_states_dailystates_daily[['fips', 'date', 'positive', 'negative', 'hospitalizedcurrently', 'hospitalized', 'hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1, factCovid_2, on = 'fips', how = 'inner')

# %%
factCovid_1.head(10)

# %%
factCovid_2.head(10)

# %%
factCovid.head(10)

# %%
factCovid.shape

# %%
#Create a Table factCovid using enigma_jhud and rearc_covid_19_testing_data_states_dailystates_daily
dimRegion_1 = enigma_jhud[['fips', 'province_state', 'country_region', 'latitude', 'longitude']]
dimRegion_2 = nytimes_data_in_usa_us_county[['fips', 'county', 'state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on = 'fips', how = 'inner')

# %%
dimRegion.head()

# %%
#Create a Table dimHospital using rearc_usa_hospital_beds
dimHospital = rearc_usa_hospital_beds[['fips', 'state_name', 'latitude', 'longtitude', 'hq_address', 'hospital_name','hospital_type', 'hq_city', 'hq_state']]

# %%
#Create a Table dimDate using rearc_covid_19_testing_data_states_dailystates_daily
dimDate = rearc_covid_19_testing_data_states_dailystates_daily[['fips','date']]

# %%
dimDate.head()

# %%
#Now i need to make some changes in date format
dimDate['date'] = pd.to_datetime(dimDate['date'], format = '%Y%m%d')

# %%
dimDate.head()

# %%
#Using the new column date after the changes to store year, month and day of week in the new columns
dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek

# %%
#Analyzing the new table
dimDate.head()

# %%
#Already created on s3
bucket = 'math-covid-de-project' 

# %%
csv_buffer = StringIO()

# %%
csv_buffer

# %%
factCovid.to_csv(csv_buffer)

# %%
#Make a login in s3 and saving the factCovid.csv to a a bucket output/
s3 = boto3.resource(
    's3',
    region_name = AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
s3.Object(bucket, 'output/factCovid.csv').put(Body = csv_buffer.getvalue())

# %%
#saving the dimDate.csv to a a bucket output/
csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3.Object(bucket, 'output/dimDate.csv').put(Body = csv_buffer.getvalue())

# %%
#saving the dimHospital.csv to a a bucket output/
dimHospital.to_csv(csv_buffer)
s3.Object(bucket, 'output/dimHospital.csv').put(Body = csv_buffer.getvalue())

# %%
#saving the dimRegion.csv to a a bucket output/
dimRegion.to_csv(csv_buffer)
s3.Object(bucket, 'output/dimRegion.csv').put(Body = csv_buffer.getvalue())

# %%
#Analyzing dimDate table schema 
dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDatesql))

# %%
#Analyzing factCovid table schema 
FactCovidsql = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
print(''.join(FactCovidsql))

# %%
#Analyzing dimRegion table schema 
dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')
print(''.join(dimRegionsql))

# %%
#Analyzing dimHospital table schema 
dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(), 'dimHospital')
print(''.join(dimHospitalsql))

# %%
#Make a login in redshift
redshift = boto3.client('redshift',
                    region_name='sa-east-1',
                    aws_access_key_id=AWS_ACCESS_KEY,
                    aws_secret_access_key=AWS_SECRET_KEY
                    )

# %%
#Connect to redshift
conn = redshift_connector.connect(
    host = 'redshift-cluster-2.cc9hvztezzps.sa-east-1.redshift.amazonaws.com',
    database = 'dev',
    user = 'awsuser',
    password = 'Passw0rd123',
    port = 5439
)

# %%
conn.autocommit = True

# %%
cursor = redshift_connector.Cursor = conn.cursor()

# %%
#Execute a query to create a table dimDate with the analyzed schema above 
cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
"fips" REAL,
"date" TIMESTAMP,
"year" INTEGER,
"month" INTEGER,
"day_of_week" INTEGER
)
""")

# %%
#Execute a query to create a table factCovid with the analyzed schema above 
cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")

# %%
#Execute a query to create a table factCovid with the analyzed schema above 
cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

# %%
#Execute a query to create a table factCovid with the analyzed schema above 
cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")

# %%
#Executing a COPY to copy the data in s3 bucket dimDate to a redshift table dimDate
cursor.execute("""
copy dimDate from 's3://math-covid-de-project/output/dimDate.csv'
credentials 'aws_iam_role=arn:aws:iam::248652697441:role/redshift-s3-access'
delimiter ','
region 'sa-east-1' 
IGNOREHEADER 1
""")

# %%
#Executing a COPY to copy the data in s3 bucket factCovid to a redshift table dimDate
cursor.execute("""
copy factCovid from 's3://math-covid-de-project/output/factCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::248652697441:role/redshift-s3-access'
delimiter ','
region 'sa-east-1' 
IGNOREHEADER 1
""")

# %%
#Executing a COPY to copy the data in s3 bucket dimRegion to a redshift table dimDate
cursor.execute("""
copy dimRegion from 's3://math-covid-de-project/output/dimRegion.csv'
credentials 'aws_iam_role=arn:aws:iam::248652697441:role/redshift-s3-access'
delimiter ','
region 'sa-east-1' 
IGNOREHEADER 1
""")

# %%
#Executing a COPY to copy the data in s3 bucket dimHospital to a redshift table dimDate
cursor.execute("""
copy dimHospital from 's3://math-covid-de-project/output/dimHospital.csv'
credentials 'aws_iam_role=arn:aws:iam::248652697441:role/redshift-s3-access'
delimiter ','
region 'sa-east-1' 
IGNOREHEADER 1
""")


