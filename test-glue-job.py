import boto3
import sys
import time
import uuid

from datetime import datetime
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Create a SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)

# sql server connections
data_table = 'dbo.client'
connection_name = 'SQL Server Indux'
transformation_ctx = 'test_transform'


# creating dataframe of table
df_datatype_test = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": data_table,
        "connectionName": connection_name,
    },
    transformation_ctx=transformation_ctx,
)

# print schema
df_datatype_test.printSchema()


# writer s3
# s3 bucket connection 
output_bucket = 's3 bucketpath'

df_datatype_test.write(
    connection_type="s3",
    connection_options={"path": "{}/test".format(output_bucket)},  
    format="parquet"
)



sc.stop()