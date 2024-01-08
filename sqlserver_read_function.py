from awsglue.context import GlueContext
from pyspark.context import SparkContext

def create_sqlserver_dataframe(table, connection_name, transformation_ctx):
    """
    Create a DynamicFrame from a SQL Server table using AWS Glue.

    Parameters:
        - table: The name of the SQL Server table (e.g., 'dbo.client').
        - connection_name: The name of the SQL Server connection.
        - transformation_ctx: The transformation context for AWS Glue.

    Returns:
        A DynamicFrame representing the SQL Server table.
    """
    sc = SparkContext()
    glueContext = GlueContext(sc)

    return glueContext.create_dynamic_frame.from_options(
        connection_type="sqlserver",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": table,
            "connectionName": connection_name,
        },
        transformation_ctx=transformation_ctx,
    )

# Example usage:
data_table = 'dbo.client'
connection_name = 'SQL Server Indux'
transformation_ctx = 'test_transform'

df_datatype_test = create_sqlserver_dataframe(data_table, connection_name, transformation_ctx)

# Print schema
df_datatype_test.printSchema()
