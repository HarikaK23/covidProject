#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install boto3


# In[2]:


import boto3
import pandas as pd
from io import StringIO
import time


# In[3]:


AWS_ACCESS_KEY = "AKIAUIVJIFBU45LJSBGZ"
AWS_SECRET_KEY = "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"
AWS_REGION = "ap-south-1"
SCHEMA_NAME = "covid_19"
S3_STAGING_DIR = "s3://athena-covid-project-output/output2"
S3_BUCKET_NAME = "athena-covid-project-output"
S3_OUTPUT_DIRECTORY = "output2"


# In[4]:


athena_client = boto3.client(
    "athena",
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name = AWS_REGION,
)


# In[5]:


Dict = {}
def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
#     print("x")
#     import pdb;pdb.set_trace()
#     print("z")
    while True:
        try:
           
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


# In[6]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM countrycode",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[7]:


response


# In[8]:


#results=athena_client.get_query_execution(QueryExecutionId = '10ef792f-8d57-476b-af68-4bde16a4450c')


# In[9]:


countrycode = download_and_load_query_results(athena_client, response)


# In[10]:


countrycode.head()


# In[11]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM countypopulation",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[12]:


countypopulation =download_and_load_query_results(athena_client, response)


# In[13]:


countypopulation.head()


# In[14]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM enigma_jhud",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[15]:


enigma_jhud = download_and_load_query_results(athena_client, response)


# In[16]:


enigma_jhud.head()


# In[17]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM enigma_nytimes_data_in_usa_us_county",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[18]:


enigma_nytimes_data_in_usa_us_county= download_and_load_query_results(athena_client, response)


# In[19]:


#enigma_nytimes_data_in_usa_us_county.head()


# In[20]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM enigma_nytimes_data_in_usa_us_states",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[21]:


enigma_nytimes_data_in_usa_us_states= download_and_load_query_results(athena_client, response)


# In[22]:


#enigma_nytimes_data_in_usa_us_states.head()


# In[23]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_states_dailystates_daily",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[24]:


rearc_covid_19_testing_data_states_dailystates_daily= download_and_load_query_results(athena_client, response)


# In[25]:


#rearc_covid_19_testing_data_states_dailystates_daily.head()


# In[26]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_us_dailyus_daily",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[27]:


rearc_covid_19_testing_data_us_dailyus_daily= download_and_load_query_results(athena_client, response)


# In[28]:


#rearc_covid_19_testing_data_us_dailyus_daily.head()


# In[29]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_covid_19_testing_data_us_total_latestus_total_latest",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[30]:


rearc_covid_19_testing_data_us_total_latestus_total_latest= download_and_load_query_results(athena_client, response)


# In[31]:


rearc_covid_19_testing_data_us_total_latestus_total_latest


# In[32]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM rearc_usa_hospital_beds",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[33]:


rearc_usa_hospital_beds= download_and_load_query_results(athena_client, response)


# In[34]:


#rearc_usa_hospital_beds.head()


# In[35]:


response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM state_abv",
    QueryExecutionContext = {"Database": SCHEMA_NAME},
    ResultConfiguration = {
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[36]:


state_abv = download_and_load_query_results(athena_client, response)


# In[37]:


state_abv.head()


# In[38]:


new_header = state_abv.iloc[0]


# In[39]:


new_header


# In[40]:


state_abv = state_abv[1:]


# In[41]:


state_abv.head()


# In[42]:


state_abv.columns = new_header


# In[43]:


state_abv.columns


# In[44]:


state_abv.head()


# In[45]:


factCovid_1 = enigma_jhud[['fips','province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active']]
factCovid_2 = rearc_covid_19_testing_data_states_dailystates_daily[['fips', 'date', 'positive', 'negative', 'hospitalizedcurrently', 'hospitalized', 'hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1, factCovid_2, on = 'fips', how='inner' )


# In[46]:


factCovid.head()


# In[47]:


factCovid.shape


# In[48]:


dimRegion_1 = enigma_jhud[['fips', 'province_state', 'country_region', 'latitude', 'longitude']]
dimRegion_2 = enigma_nytimes_data_in_usa_us_county[['fips', 'county', 'state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='fips', how='inner')


# In[49]:


dimRegion.head()


# In[50]:


dimHospital = rearc_usa_hospital_beds[['fips', 'state_name', 'latitude', 'longtitude', 'hq_address', 'hospital_name', 'hospital_type', 'hq_city', 'hq_state']]


# In[51]:


dimHospital.head()


# In[52]:


dimDate = rearc_covid_19_testing_data_states_dailystates_daily[['fips', 'date']]


# In[53]:


dimDate.head()


# In[54]:


dimDate['date']= pd.to_datetime(dimDate['date'], format='%Y%m%d')


# In[55]:


dimDate.head()


# In[56]:


dimDate['year'] = dimDate['date'].dt.year
dimDate['month']= dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek


# In[57]:


dimDate.head()


# In[58]:


bucket = 'harika-de-project-covid-output'


# In[59]:


csv_buffer = StringIO()


# In[60]:


csv_buffer


# In[61]:


factCovid.to_csv(csv_buffer)


# In[62]:


#s3_resource = boto3.resource('s3')
#s3_resource.Object(bucket, 'output/factCovid.csv').put(Body = csv_buffer.getvalue())


# In[ ]:


csv_buffer.getvalue()


# In[ ]:


import boto3
from pprint import pprint
import pathlib
import os
def upload_file_using_client():
    """
    Uploads file to S3 bucket using S3 client object
    :return: None
    """
    s3 = boto3.client("s3")
    bucket_name = "harika-de-project-covid-output"
    object_name = "factCovid.csv"
    file_name = os.path.join(pathlib.Path(__file__).parent.resolve(), "factCovid.csv")
    response = s3.upload_file(file_name, bucket_name, object_name)
print(response) 


# In[ ]:


def upload_file_using_resource():
    """
    Uploads file to S3 bucket using S3 resource object.
    This is useful when you are dealing with multiple buckets st same time.
    :return: None
    """
    s3 = boto3.resource("s3")
    bucket_name = "harika-de-project-covid-output"
    object_name = "sample2.txt"
    file_name = os.path.join(pathlib.Path(__file__).parent.resolve(), "sample_file.txt")
    bucket = s3.Bucket(bucket_name)
    response = bucket.upload_file(file_name, object_name)
print(response)  # Prints None


# In[ ]:


df = pd.DataFrame.from_dict(factCovid)
data = df.drop_duplicates()
data.head()
data.to_csv('C:/Users/DELL/Desktop/Harika_DE_projects/factCovid.csv')


# In[ ]:


# df = pd.DataFrame.from_dict(dimRegion)
# data = df.drop_duplicates()
# # data.head()
# data.to_csv('C:/Users/DELL/Desktop/Harika_DE_projects/dimRegion.csv')


# In[ ]:


# df = pd.DataFrame.from_dict(dimHospital)
# data = df.drop_duplicates()
# # data.head()
# data.to_csv('C:/Users/DELL/Desktop/Harika_DE_projects/dimHospital.csv')


# In[ ]:


# df = pd.DataFrame.from_dict(dimDate)
# data = df.drop_duplicates()
# # data.head()
# data.to_csv('C:/Users/DELL/Desktop/Harika_DE_projects/dimDate.csv')


# In[65]:


dimDate.head()


# In[64]:


dimDate.head()


# In[ ]:


dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDatesql))


# In[ ]:




