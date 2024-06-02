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

# Script generated for node Accelerometer
Accelerometer_node1717314306133 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_node1717314306133")

# Script generated for node Customer_Trusted
Customer_Trusted_node1717314543537 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_Trusted_node1717314543537")

# Script generated for node Join
Join_node1717314619825 = Join.apply(frame1=Accelerometer_node1717314306133, frame2=Customer_Trusted_node1717314543537, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1717314619825")

# Script generated for node Amazon S3
AmazonS3_node1717314413250 = glueContext.write_dynamic_frame.from_options(frame=Join_node1717314619825, connection_type="s3", format="json", connection_options={"path": "s3://tanlx-bucket/accelerometer/accelerometer_trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717314413250")

job.commit()