import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [stream_type = kafka, stream_batch_time = "100 seconds", database = "aws_glue_db", additionalOptions = {"startingOffsets": "earliest", "inferSchema": "false"}, table_name = "msk_covidtweets"]
## @return: datasource0
## @inputs: []
data_frame_datasource0 = glueContext.create_data_frame.from_catalog(database = "aws_glue_db", table_name = "msk_covidtweets", transformation_ctx = "datasource0", additional_options = {"startingOffsets": "earliest", "inferSchema": "false"})
def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        datasource0 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        ## @type: ApplyMapping
        ## @args: [mapping = [("id_str", "string", "id_str", "string"), ("created_at", "string", "created_at", "string"), ("source", "string", "source", "string"), ("text", "string", "text", "string"), ("`user.location`", "string", "user_location", "string"), ("`entities.hashtags[0].text`", "string", "hashtags2", "string"), ("`entities.hashtags[1].text`", "string", "hashtags1", "string"), ("lang", "string", "lang", "string"), ("`coordinates.coordinates[0]`", "string", "coordinates0", "string"), ("`coordinates.coordinates[1]`", "string", "coordinates1", "string")], transformation_ctx = "applymapping0"]
        ## @return: applymapping0
        ## @inputs: [frame = datasource0]
        applymapping0 = ApplyMapping.apply(frame = datasource0, mappings = [("id_str", "string", "id_str", "string"), ("created_at", "string", "created_at", "string"), ("source", "string", "source", "string"), ("text", "string", "text", "string"), ("`user.location`", "string", "user_location", "string"), ("`entities.hashtags[0].text`", "string", "hashtags2", "string"), ("`entities.hashtags[1].text`", "string", "hashtags1", "string"), ("lang", "string", "lang", "string"), ("`coordinates.coordinates[0]`", "string", "coordinates0", "string"), ("`coordinates.coordinates[1]`", "string", "coordinates1", "string")], transformation_ctx = "applymapping0")
        ## @type: DataSink
        ## @args: [database = "aws_glue_db", table_name = "streaming_data_public_msk_tweets", stream_batch_time = "100 seconds", redshift_tmp_dir = TempDir, transformation_ctx = "datasink1"]
        ## @return: datasink1
        ## @inputs: [frame = applymapping0]

        datasink1 = glueContext.write_dynamic_frame.from_catalog(frame = applymapping0, database = "aws_glue_db", table_name = "streaming_data_public_msk_tweets", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink1")
glueContext.forEachBatch(frame = data_frame_datasource0, batch_function = processBatch, options = {"windowSize": "5 seconds", "checkpointLocation": args["TempDir"] + "/checkpoint/"})
job.commit()