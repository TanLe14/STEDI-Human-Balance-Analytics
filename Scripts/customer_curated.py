import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1717316593023 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/customer/trusted"], "recurse": True}, transformation_ctx="customer_trusted_node1717316593023")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1717424860726 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/accelerometer/accelerometer_trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1717424860726")

# Script generated for node Join
Join_node1717424933225 = Join.apply(frame1=customer_trusted_node1717316593023, frame2=accelerometer_trusted_node1717424860726, keys1=["email"], keys2=["email"], transformation_ctx="Join_node1717424933225")

# Script generated for node Amazon S3
AmazonS3_node1717316606499 = glueContext.write_dynamic_frame.from_options(frame=Join_node1717424933225, connection_type="s3", format="json", connection_options={"path": "s3://tanlx-bucket/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717316606499")

job.commit()