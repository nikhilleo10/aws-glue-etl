#!/usr/bin/env python
# coding: utf-8

# In[ ]:



import sys
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Join, SelectFields
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
import scispacy
import spacy
import random
nlp = spacy.load("en_ner_bc5cdr_md")
import boto3
import pandas as pd
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("data-eng-updated")

print('Updated file locally')

from pyspark.sql.functions import udf

import nltk
nltk.download('vader_lexicon') # download the pre-trained model

from nltk.sentiment import SentimentIntensityAnalyzer

# create an instance of the pre-trained sentiment analyzer
sia = SentimentIntensityAnalyzer()

sentence = "Amazing movie, I really enjoyed it"

# use the sentiment analyzer to get a sentiment score for the sentence
sentiment = sia.polarity_scores(sentence)

# print the sentiment score
print(sentiment["pos"])
customer_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database='data-eng-s3-demo-db', table_name='test_customers',transformation_ctx = "datasource1" ,additional_options = {"jobBookmarkKeys":["id"],"jobBookmarkKeysSortOrder":"asc"})
customer_data_frame = customer_dynamic_frame.toDF()
customer_data_frame.printSchema()
wearable_details = glueContext.create_dynamic_frame.from_catalog(database='data-eng-s3-demo-db', table_name='test_wearables',transformation_ctx = "datasource0", additional_options = {"jobBookmarkKeys":["user_id"],"jobBookmarkKeysSortOrder":"asc"})
wearable_df = wearable_details.toDF()
wearable_df.show()
providers_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database='data-eng-s3-demo-db', table_name='provider',transformation_ctx = "datasource0" ,additional_options = {"jobBookmarkKeys":["id"],"jobBookmarkKeysSortOrder":"asc"})
providers_data_frame = providers_dynamic_frame.toDF()
providers_dynamic_frame.printSchema()
# Rename device_id to device_model
wearable_df = wearable_df.withColumnRenamed('device_id','device_model')
wearable_df.persist()
# Create distincnt device table
distinct_device_df = wearable_df.select('device_model','brand').distinct()
# # Define a UDF to generate UUIDs
generate_uuid = udf(lambda: str(uuid.uuid4().hex), StringType())

# # Add a new column with a unique UUID for each row
# distinct_device_df = distinct_device_df.withColumn("device_id", generate_uuid())
# distinct_device_df.persist()
distinct_device_df.show(100)
# joined_devices_and_wearables = wearable_df.join(distinct_device_df, ['device_model', 'brand'], 'left')
watch_facts_table = wearable_df.select(
    'user_id',
    'device_model',
    'date',
    'steps', 
    'heart_rate', 
    'calories_burned', 
    'sleep_hours', 
    'active_minutes', 
    'diastolic_bp', 
    'body_temp', 
    'oxygen_saturation', 
    'respiratory_rate',
)
user_table = customer_data_frame.select(
    'id',
    'age',
    'gender',
    'bmi',
    'smoking',
    'alcohol',
    'family_history',
    'pre_existing_conditions',
    'medications',
    'hospitalization',
    'blood_sugar',
    'cholesterol',
    'blood_pressure',
    'vitamin_deficiencies',
    'mental_health',
    
    'occupation',
    'work_environment',
    'travel_history',
    'geographic_location',
    'environmental_exposure',
    'exercise_frequency',
    'dietary_habits',
    
    'marital_status',
    'education_level',
    'income_level',
    'employment_status',
    'dental_health',
    'allergies_sensitivities',
    'health_plan_type',
    'deductible'
).distinct()
renewal_history_facts_table = customer_data_frame.select(
  'id',
  'year',
  'plan_type',
  'cancellation_reason',
  'premium_cost',
  'health_plan_type',
  'deductible'
)

renewal_history_facts_table = renewal_history_facts_table.withColumnRenamed('id','user_id')

# Add a new column with a unique UUID for each row
renewal_history_facts_table = renewal_history_facts_table.withColumn("id", generate_uuid())
renewal_history_facts_table.persist()
def getCurrentTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time
def uploadToRedshift(df, tableName):
    try:
        df.toDF().write.format("jdbc").\
        option("url", "jdbc:redshift://health-space.511522223657.ap-south-1.redshift-serverless.amazonaws.com:5439/dev").\
        option("dbtable", f"public.{tableName}").\
        option("user", "admin").\
        option("password", "RedshiftPassword99").\
        mode('append').save()
        print(f"Successfully created {tableName} table to Redshift")
    except Exception as e:
        print(f"Error creating {tableName} table to Redshift:", str(e))
        raise e
def insertToRedshift(df,tableName):
    destinationTable = f"public.{tableName}"
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = df,
        catalog_connection="redshift-new-conn-data-eng",
        connection_options = {
            "database":"dev",
            "dbtable":destinationTable,
            
        },
        redshift_tmp_dir="s3://data-eng-s3-wednesday/tmp/tmpdir-r/",
        transformation_ctx="upsert_to_redshift"
    )

def upsertToRedshift2(df,tableName,pk):
    destinationTable = f"public.{tableName}"
    destination = f"dev.{destinationTable}"
    staging = f"dev.{destinationTable}_staging"
    stg = f"{destinationTable}_staging"
    
    fields = df.toDF().columns
    print(fields)

    
    
    postActions = f"""
    DELETE FROM {destination} USING {staging} AS S WHERE {destinationTable}.{pk} = S.{pk};
    INSERT INTO {destination} SELECT * FROM {staging};DROP TABLE IF EXISTS {staging}"""
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = df,
        catalog_connection="redshift-new-conn-data-eng",
        connection_options = {
            "database":"dev",
            "dbtable":stg,
            "postActions":postActions,
            
        },
        redshift_tmp_dir="s3://data-eng-s3-wednesday/tmp/tmpdir-r/",
        transformation_ctx="upsert_to_redshift"
    )
        
    
    
def upsertToRedshift2Multi(df,tableName,pk,sk):
    destinationTable = f"public.{tableName}"
    destination = f"dev.{destinationTable}"
    staging = f"dev.{destinationTable}_staging"
    stg = f"{destinationTable}_staging"
    
    fields = df.toDF().columns
    print(fields)

    
    
    postActions = f"""
    DELETE FROM {destination} USING {staging} AS S WHERE {destinationTable}.{pk} = S.{pk} and {destinationTable}.{sk} = S.{sk};
    INSERT INTO {destination} SELECT * FROM {staging};DROP TABLE IF EXISTS {staging}"""
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = df,
        catalog_connection="redshift-new-conn-data-eng",
        connection_options = {
            "database":"dev",
            "dbtable":stg,
            "postActions":postActions,
            
        },
        redshift_tmp_dir="s3://data-eng-s3-wednesday/tmp/tmpdir-r/",
        transformation_ctx="upsert_to_redshift"
    )
        
empty_device_frame = DynamicFrame.fromDF(distinct_device_df.select('*').limit(0),glueContext, "watch_facts_data_frame")
uploadToRedshift(empty_device_frame,'devices')


print(f"Start Time: {getCurrentTime()}")
distinct_device_dy_frame = DynamicFrame.fromDF(distinct_device_df.select('*'), glueContext, "watch_facts_data_frame")

upsertToRedshift2(distinct_device_dy_frame, 'devices','device_model')
print(f"End Time: {getCurrentTime()}")
watch_facts_table.show()
empty_device_frame = DynamicFrame.fromDF(watch_facts_table.select('*').limit(0),glueContext, "watch_facts_data_frame")
uploadToRedshift(empty_device_frame,'watch_facts')

print(f"Start Time: {getCurrentTime()}")
watch_facts_dy_frame = DynamicFrame.fromDF(watch_facts_table.select('*'), glueContext, "watch_facts_data_frame")
print(f"Start Time 2: {getCurrentTime()}")
upsertToRedshift2Multi(watch_facts_dy_frame,'watch_facts','user_id','date')
print(f"End Time: {getCurrentTime()}")
empty_users_frame = DynamicFrame.fromDF(user_table.select('*').limit(0),glueContext, "users_data_frame")
uploadToRedshift(empty_users_frame,'users')
user_table.printSchema()
print(f"Start Time: {getCurrentTime()}")
users_dy_frame = DynamicFrame.fromDF(user_table.select('*'), glueContext, "users_data_frame")
upsertToRedshift2(users_dy_frame, 'users','id')

print(f"End Time: {getCurrentTime()}")
renewal_empty_frame = DynamicFrame.fromDF(renewal_history_facts_table.select('*').limit(0),glueContext, "users_data_frame")
uploadToRedshift(renewal_empty_frame, 'renewal_history')
print(f"Start Time: {getCurrentTime()}")
renewal_history_dy_frame = DynamicFrame.fromDF(renewal_history_facts_table, glueContext, "renewal_history_data_frame")
upsertToRedshift2(renewal_history_dy_frame, 'renewal_history','id')
print(f"End Time: {getCurrentTime()}")
provider_empty_frame = DynamicFrame.fromDF(providers_data_frame.select('*').limit(0),glueContext, "providers_data_frame")
uploadToRedshift(provider_empty_frame, 'provider')
print(f"Start Time: {getCurrentTime()}")
provider_dy_frame = DynamicFrame.fromDF(providers_data_frame, glueContext, "providers_data_frame")
upsertToRedshift2(provider_dy_frame, 'provider','id')
print(f"End Time: {getCurrentTime()}")
s3 = boto3.resource('s3')
bucket_name = 'data-eng-s3-wednesday'
prefix = 'notes/admission_notes/'  # Include trailing slash
s3_bucket = s3.Bucket(bucket_name)

admission_texts = []

for obj in s3_bucket.objects.filter(Prefix=prefix):
    if obj.key.endswith('.txt'):
        s3_object = s3.Object(bucket_name, obj.key)
        file_content = s3_object.get()['Body'].read().decode('utf-8')
        individual_texts = file_content.split('\n')  # Or any other delimiter that separates texts
        individual_texts = [x for x in individual_texts if x != ""]
        admission_texts.extend(individual_texts)
    
user_spark = DynamicFrame.fromDF(user_table.select('*'),glueContext, "users_data_frame").toDF()
provider_spark = DynamicFrame.fromDF(providers_data_frame.select('*'),glueContext,"providers_data_frame").toDF()




    
    
u = user_spark.select('id')
getNewAdmissionText = udf(lambda: admission_texts[random.randint(0, len(admission_texts)-1)])
u = u.withColumnRenamed('id','user_id')
providerId = udf(lambda: random.randint(1, 2000))
notesType = udf(lambda: "admission_notes" )
def getNewAdmissionTextSentiment(s):
    a = sia.polarity_scores(s)
    return a["compound"]+a["neu"]
getNewAdmissionTextSentiment_udf = udf(getNewAdmissionTextSentiment, DoubleType())
u = u.withColumn('notes', getNewAdmissionText())
u = u.withColumn('provider_id', providerId())
u = u.withColumn('notes_pos_sentiment', getNewAdmissionTextSentiment_udf(col('notes')))
u = u.withColumn('id', generate_uuid())
u = u.withColumn('notes_type',notesType())
u.persist()
u.show()


s3 = boto3.resource('s3')
bucket_name = 'data-eng-s3-wednesday'
prefix = 'notes/admission_follow_up/'  # Include trailing slash
s3_bucket = s3.Bucket(bucket_name)

follow_up_texts = []

for obj in s3_bucket.objects.filter(Prefix=prefix):
    if obj.key.endswith('.txt'):
        s3_object = s3.Object(bucket_name, obj.key)
        file_content = s3_object.get()['Body'].read().decode('utf-8')
        individual_texts = file_content.split('----------------------------------------------------------------------------------------------------')  # Or any other delimiter that separates texts
        individual_texts = [x for x in individual_texts if x != ""]
        follow_up_texts.extend(individual_texts)

t = user_spark.select('id')
getNewFollowUpText = udf(lambda: follow_up_texts[random.randint(0, len(follow_up_texts)-1)])
t = t.withColumnRenamed('id','user_id')

notesTypeFollowUp = udf(lambda: "follow_up_notes" )

    
getNewAdmissionTextSentiment_udf = udf(getNewAdmissionTextSentiment, DoubleType())
t = t.withColumn('notes', getNewFollowUpText())
t = t.withColumn('provider_id', providerId())
t = t.withColumn('notes_pos_sentiment', getNewAdmissionTextSentiment_udf(col('notes')))
t = t.withColumn('id', generate_uuid())
t = t.withColumn('notes_type',notesType())
t.persist()

s3 = boto3.resource('s3')
bucket_name = 'data-eng-s3-wednesday'
prefix = 'notes/discharge_notes/'  # Include trailing slash
s3_bucket = s3.Bucket(bucket_name)

discharge_texts = []

for obj in s3_bucket.objects.filter(Prefix=prefix):
    if obj.key.endswith('.txt'):
        s3_object = s3.Object(bucket_name, obj.key)
        file_content = s3_object.get()['Body'].read().decode('utf-8')
        individual_texts = file_content.split('-------------------------------------------------------')  # Or any other delimiter that separates texts
        individual_texts = [x for x in individual_texts if x != ""]
        discharge_texts.extend(individual_texts)
v = user_spark.select('id')
getNewDischargeText = udf(lambda: follow_up_texts[random.randint(0, len(discharge_texts)-1)])
v = v.withColumnRenamed('id','user_id')

notesTypeDischarge = udf(lambda: "discharge_notes" )

    
getNewAdmissionTextSentiment_udf = udf(getNewAdmissionTextSentiment, DoubleType())
v = v.withColumn('notes', getNewDischargeText())
v = v.withColumn('provider_id', providerId())
v = v.withColumn('notes_pos_sentiment', getNewAdmissionTextSentiment_udf(col('notes')))
v = v.withColumn('id', generate_uuid())
v = v.withColumn('notes_type',notesTypeDischarge())
v = v.withColumn('notes', regexp_replace('notes', '\n', ''))
v.select('*').show()
v.persist()
all_notes_df = u.union(t).union(v)
all_notes_df.persist()
all_notes_df.show()
# notes_df.where(notes_df.notes_type == 'discharge_notes')

# notes_df.persist()
# notes_df.count()

# v.show()

notes_empty_frame = DynamicFrame.fromDF(all_notes_df,glueContext, "doctor_notes_data_frame")
insertToRedshift(notes_empty_frame, 'doctor_notes')
# print(f"Start Time: {getCurrentTime()}")
# renewal_history_dy_frame = DynamicFrame.fromDF(renewal_history_facts_table, glueContext, "renewal_history_data_frame")
# upsertToRedshift2(renewal_history_dy_frame, 'renewal_history','id')
# print(f"End Time: {getCurrentTime()}")
print('Done with script, updated file name')
job.commit()

# In[ ]:



