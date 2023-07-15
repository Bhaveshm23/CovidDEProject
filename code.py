! pip install boto3
! pip install redshift_connector
import boto3
import pandas as pd
from io import StringIO
import time # for sleep
# using redshift connector to connect to redshfit
import redshift_connector

# Defining static values
AWS_ACCESS_KEY = '<your-access-key>'
AWS_SECRET_KEY = '<your-secret-key>'
AWS_REGION = 'us-east-1'
SCHEMA_NAME = 'covid19db'
S3_STAGING_DIR = '<s3-athena-bucket-name>/output/'
S3_BUCKET_NAME = '<s3-athena-bucket-name>'
S3_OUTPUT_DIRECTORY = 'output'


# connect to Athena
athena_client = boto3.client(
    "athena",
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name = AWS_REGION,
)

# The below fucntion takes boto3  object, run on athena and store output in s3
Dict = {}
def download_and_load_query_results(
    client:boto3.client,query_response:Dict
) -> pd.DataFrame:
  while True:
    try:
        # This function only loads the first 1000 rows
        client.get_query_results(
            QueryExecutionId=query_response["QueryExecutionId"]
        )
        break
    except Exception as err:
        if "not yet finished" in str(err):
            time.sleep(0.001)
        else:
            raise err
  temp_file_location: str = "athena_query_results.csv"
  s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
  )
  s3_client.download_file(
      S3_BUCKET_NAME,
      f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
      temp_file_location,
  )
  return pd.read_csv(temp_file_location)

# Running query on all the tables in Athena and storing data in S3. Stores CSV data and meta data.
response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM enigma_jhu",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

print(response)
enigma_jhu = download_and_load_query_results(athena_client,response)
enigma_jhu.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM nytimes_data_in_usa_us_states",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

nytimes_data_in_usa_us_states = download_and_load_query_results(athena_client,response)
nytimes_data_in_usa_us_states.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM nytimes_data_in_usaus_county",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

nytimes_data_in_usaus_county = download_and_load_query_results(athena_client,response)
nytimes_data_in_usaus_county.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_states_daily",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

rearc_covid_19_testing_data_states_daily = download_and_load_query_results(athena_client,response)
rearc_covid_19_testing_data_states_daily.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_us_daily",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

rearc_covid_19_testing_data_us_daily = download_and_load_query_results(athena_client,response)
rearc_covid_19_testing_data_us_daily.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_us_total_latest",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

rearc_covid_19_testing_data_us_total_latest = download_and_load_query_results(athena_client,response)
rearc_covid_19_testing_data_us_total_latest.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_usa_hospital_beds",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

rearc_usa_hospital_beds = download_and_load_query_results(athena_client,response)
rearc_usa_hospital_beds.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM static_datasets_countrycode",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

static_datasets_countrycode = download_and_load_query_results(athena_client,response)
static_datasets_countrycode.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM static_datasets_countypopulation",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

static_datasets_countypopulation = download_and_load_query_results(athena_client,response)
static_datasets_countypopulation.head()

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM static_datasets_state_abv",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
      "OutputLocation" : S3_STAGING_DIR,
      "EncryptionConfiguration" :{"EncryptionOption":"SSE_S3"},
    },
)

static_datasets_state_abv = download_and_load_query_results(athena_client,response)
static_datasets_state_abv.head()



# the above table stores column headers as row 0, fixing this below
new_header = static_datasets_state_abv.iloc[0] # get the first row
static_datasets_state_abv = static_datasets_state_abv[1:] # take daat other then 1st row
static_datasets_state_abv.columns = new_header #set the header to first row
static_datasets_state_abv.head()

 # ETL JOB
# Convert the relational data model into dimensional model

factCovid_1 = enigma_jhu[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
factCovid_2 = rearc_covid_19_testing_data_states_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1,factCovid_2,on='fips',how='inner')

factCovid.head()

# # change the date type of fact covid so that it is in the redshift format

# factCovid['date'] = pd.to_datetime(factCovid['date'],format="%Y%m%d")
# factCovid.head()



dimRegion_1 = enigma_jhu[['fips','province_state','country_region','latitude','longitude']]
dimRegion_2 = nytimes_data_in_usaus_county[['fips','county','state']]
dimRegion = pd.merge(dimRegion_1,dimRegion_2,on='fips',how='inner')
dimRegion.head()

dimHospital = rearc_usa_hospital_beds[['fips','state_name','latitude','longtitude','hq_address','hospital_name','hq_state','hospital_type','hq_city']]
dimHospital.head()

dimDate = rearc_covid_19_testing_data_states_daily[['fips','date']]
dimDate.head()

dimDate['date'] = pd.to_datetime(dimDate['date'],format='%Y%m%d')
dimDate.head()

dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek
dimDate.head()

from botocore import regions
 # Storing the output in S3

output_bucket = '<output-bucket-name>
csv_buffer = StringIO() # since we want to put value in binary format

factCovid.to_csv(csv_buffer)

s3_resource = boto3.resource('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
s3_resource.Object(output_bucket, 'output/factCovid.csv').put(Body=csv_buffer.getvalue())


dimRegion.to_csv(csv_buffer)
s3_resource = boto3.resource('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
s3_resource.Object(output_bucket, 'output/dimRegion.csv').put(Body=csv_buffer.getvalue())



dimHospital.to_csv(csv_buffer)
s3_resource = boto3.resource('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
s3_resource.Object(output_bucket, 'output/dimHospital.csv').put(Body=csv_buffer.getvalue())


dimDate.to_csv(csv_buffer)
s3_resource = boto3.resource('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
s3_resource.Object(output_bucket, 'output/dimDate.csv').put(Body=csv_buffer.getvalue())

# LOADING data into REDSHIFT
# extract schema from df use -> io.sql.get_schema

dimDateSql = pd.io.sql.get_schema(dimDate.reset_index(),'dimDate')
print(''.join(dimDateSql))

factCovidSql = pd.io.sql.get_schema(factCovid.reset_index(),'factCovid')
print(''.join(factCovidSql))

dimRegionSql = pd.io.sql.get_schema(dimRegion.reset_index(),'dimRegion')
print(''.join(dimRegionSql))

dimHospitalSql = pd.io.sql.get_schema(dimHospital.reset_index(),'dimHospital')
print(''.join(dimHospitalSql))

#***Below can be written in a glue job***



 # connect to database on redshfit

conn = redshift_connector.connect(
    host = 'redshift-cluster-1.clf2j1qoptop.us-east-1.redshift.amazonaws.com', # hostname -> name of end point make sure to remove: port and db name from it
    database = '<edshift cluster databse name>',
    user = '<redshift cluster username>',
    password = '<redshift cluster passowrd>'
)

# set autocommit as true
conn.autocommit = True
cursor = redshift_connector.Cursor = conn.cursor()

# creating table on redshift
cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
"fips" INTEGER,
"date" TIMESTAMP,
"year" INTEGER,
"month" INTEGER,
"day_of_week" INTEGER
)
""")

cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" INTEGER,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
               """)


cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" INTEGER,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
               """)

cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" INTEGER,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
               """)

# copy data from s3 to redshift
cursor.execute("""
copy dimDate from '<s3-output-bucket>/output/dimDate.csv'
iam_role 'aws_iam_role='arn:aws:iam::474557776735:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1 """)


cursor.execute("""
copy dimHospital from '<s3-output-bucket>/output/dimHospital.csv'
iam_role 'aws_iam_role='arn:aws:iam::474557776735:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1
""")


cursor.execute("""
copy factCovid from '<s3-output-bucket>/output/factCovid.csv'
iam_role 'aws_iam_role='arn:aws:iam::474557776735:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1
""")


cursor.execute("""
copy dimRegion from '<s3-output-bucket>/output/dimRegion.csv'
iam_role 'aws_iam_role='arn:aws:iam::474557776735:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1
""")
