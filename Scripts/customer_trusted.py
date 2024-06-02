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

# Script generated for node customer_landing
customer_landing_node1717302192561 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1717302192561")

# Script generated for node customer_filter
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
customer_filter_node1717303093768 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1717302192561}, transformation_ctx = "customer_filter_node1717303093768")

# Script generated for node customer_trusted
customer_trusted_node1717302200790 = glueContext.write_dynamic_frame.from_options(frame=customer_filter_node1717303093768, connection_type="s3", format="json", connection_options={"path": "s3://tanlx-bucket/customer/trusted/", "partitionKeys": []}, transformation_ctx="customer_trusted_node1717302200790")

job.commit()