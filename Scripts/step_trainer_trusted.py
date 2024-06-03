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

# Script generated for node customer_curated
customer_curated_node1717324168411 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1717324168411")

# Script generated for node step_trainer_landing
step_trainer_landing_node1717324169766 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tanlx-bucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1717324169766")

# Script generated for node Join
Join_node1717324172934 = Join.apply(frame1=step_trainer_landing_node1717324169766, frame2=customer_curated_node1717324168411, keys1=["serialNumber"], keys2=["serialNumber"], transformation_ctx="Join_node1717324172934")

# Script generated for node Drop Fields
DropFields_node1717325480253 = DropFields.apply(frame=Join_node1717324172934, paths=["`.serialNumber`", "distanceFromObject"], transformation_ctx="DropFields_node1717325480253")

# Script generated for node Amazon S3
AmazonS3_node1717324188394 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1717325480253, connection_type="s3", format="json", connection_options={"path": "s3://tanlx-bucket/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717324188394")

job.commit()