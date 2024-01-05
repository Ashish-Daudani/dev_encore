sc.stop()
from awsglue.context import GlueContext
from pyspark.context import SparkContext 
sc = SparkContext()
glueContext = GlueContext(sc)
data_table = 'dbo.client'
connection_name = 'SQL Server Indux'
transformation_ctx = 'test_transform'

df_connection_test = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": data_table,
        "connectionName": connection_name,
    },
    transformation_ctx=transformation_ctx,
)

# df_connection_test.printSchema()
df_check = df_connection_test.toDF()

df_check.show(5)

if df_check.count() != 0 :
    print('Connection Successful')
else:
    print('Error')