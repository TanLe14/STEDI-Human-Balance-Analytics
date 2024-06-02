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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1717326128718 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1717326128718")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1717326130772 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/accelerometer/accelerometer_trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1717326130772")

# Script generated for node Join
Join_node1717326134156 = Join.apply(frame1=step_trainer_trusted_node1717326128718, frame2=accelerometer_trusted_node1717326130772, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1717326134156")

# Script generated for node Amazon S3
AmazonS3_node1717326140277 = glueContext.write_dynamic_frame.from_options(frame=Join_node1717326134156, connection_type="s3", format="json", connection_options={"path": "s3://tanlx-bucket/machine_learning_curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717326140277")

job.commit()