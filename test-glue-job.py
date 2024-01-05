import boto3
import sys
import time
import uuid

from datetime import datetime
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import input_file_name
from pyspark.context import SparkContextt
from pyspark.sql import SparkSession


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


# writer s3..........................................
# s3 bucket connection 
output_bucket = 's3 bucketpath'

df_datatype_test.write(
    connection_type="s3",
    connection_options={"path": "{}/test".format(output_bucket)},  
    format="parquet"
)

# s3_connection = {
#     "path" : "s3 bucket path",
#     "format" : "csv"
# }

# s3_dynamic_frame = glueContext.create_dynamic_frame.from_option(
#     connection_type = 's3',
#     connection_options = s3_connection
# )
# s3_dynamic_frame.show()

# read from s3 using glue context
s3_path = "s3 path"
df_s3_load = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [s3_path], "recurse": True, "header" : True, "inferschema" : True})

df_s3_load.show(10)


# df_s3_data = spark.read.format('csv').load(s3_path)

# sc.stop()
       

# Reading data from redshift..........................

sc = SparkContext()
spark = SparkSession.builder.appName("example").getOrCreate()

glueContext = GlueContext(spark.sparkContext)

# Connections 
data_table_redshift = "datatype_test.cdc_status"
temp_dir_redshift = "s3://bte-lakeformation/glue-assets/redshift-tempdir/" 
redshift_connection_name = "Redshift Prod"

connection_options = {
    "useConnectionProperties": "true",
    "dbtable": data_table_redshift,
    "redshiftTmpDir": temp_dir_redshift,
    "connectionName": redshift_connection_name,
    
}

redshift_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options=connection_options
)

# display result
redshift_dynamic_frame.printSchema()
redshift_dynamic_frame.show()
sc.stop()


# writer to redshift ..............................

from awsglue.dynamicframe import DynamicFrame

# data = [
#     (1, 'tab1'),
#     (2, 'tab2')
# ]
# schema = ["id", "name"]
# demo_df = spark.createDataFrame(data, schema=schema)
# demo = DynamicFrame.fromDF(demo_df, glueContext, "demo")

redshift_write_options = {
    "useConnectionProperties": "true",
    "dbtable": "datatype_test.newtable",  # Replace with your Redshift table name
    "redshiftTmpDir": "s3://bte-lakeformation/glue-assets/redshift-tempdir/",
    "connectionName": "Redshift Prod",
    # Include other necessary options, such as aws_iam_role if required
}

# Write the transformed DynamicFrame to Redshift using from_options
glueContext.write_dynamic_frame.from_options(
    frame=your_dynamic_frame,
    connection_type="redshift",
    connection_options=redshift_write_options
).write()


