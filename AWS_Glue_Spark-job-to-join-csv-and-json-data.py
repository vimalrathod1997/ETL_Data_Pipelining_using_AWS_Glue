import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1652899056744 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleansed_data",
    table_name="clean_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1652899056744",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1652898939740 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleansed_data",
    table_name="cleansed_statistics_data_csv_data",
    transformation_ctx="AWSGlueDataCatalog_node1652898939740",
)

# Script generated for node Join
Join_node1652898977209 = Join.apply(
    frame1=AWSGlueDataCatalog_node1652898939740,
    frame2=AWSGlueDataCatalog_node1652899056744,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1652898977209",
)

# Script generated for node Amazon S3
AmazonS3_node1652899191691 = glueContext.getSink(
    path="s3://bigdata-on-youtube-analytics-ap-south-1-762644331473-dev/youtube/report_youtube_category_statistics_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1652899191691",
)
AmazonS3_node1652899191691.setCatalogInfo(
    catalogDatabase="db_youtube_analytics",
    catalogTableName="report_youtube_category_statistics_data",
)
AmazonS3_node1652899191691.setFormat("glueparquet")
AmazonS3_node1652899191691.writeFrame(Join_node1652898977209)
job.commit()
