import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from data_eng_updated_new import transform_to_follow_up_notes
from pyspark.sql.functions import col

from pyspark.sql.functions import udf

def test_transform_to_discharge_notes():
    # Create a test DataFrame with one row
    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField("id", IntegerType())
    ])
    df = spark.createDataFrame(data=[[1]],schema= schema)

    # Define the expected output
    expected_schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("notes", StringType()),
        StructField("provider_id", StringType()),
        StructField("notes_pos_sentiment", StringType()),
        StructField("id", StringType()),
        StructField("notes_type", StringType())
    ])
    expected_output = spark.createDataFrame(data=[[1, 'some followup text', 400, 0.5, 'some_id', 'follow_up_notes']], schema=expected_schema)

    # Call the function and check the output
    getNewFollowUpText = udf(lambda: "some followup text")
    actual_output = transform_to_follow_up_notes(df,getNewFollowUpText)
    assert expected_output.select(col("notes_type")).first() == actual_output.select(col("notes_type")).first()
    assert 10==10