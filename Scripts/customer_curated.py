import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1717316593023 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/customer/trusted"], "recurse": True}, transformation_ctx="customer_trusted_node1717316593023")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where email is not null

'''
SQLQuery_node1717316603972 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_trusted_node1717316593023}, transformation_ctx = "SQLQuery_node1717316603972")

# Script generated for node Amazon S3
AmazonS3_node1717316606499 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1717316603972, connection_type="s3", format="json", connection_options={"path": "s3://tanlx-bucket/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717316606499")

job.commit()