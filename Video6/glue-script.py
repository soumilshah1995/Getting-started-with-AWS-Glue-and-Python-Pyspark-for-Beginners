try:
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.sql.functions import udf
    import hashlib
    from pyspark.sql.functions import concat_ws, udf, concat
    from awsglue.dynamicframe import DynamicFrame

except Exception as e:
    pass

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


@udf("String")
def hasher(x):
    try:
        data = hashlib.md5(repr(x).encode("UTF-8")).hexdigest().__str__()
        return data
    except Exception as e:
        return ""


readDf = glueContext.create_dynamic_frame.from_catalog(
    database="learndb",
    table_name="soumil_data",
    transformation_ctx="readDf",
)

df = readDf.toDF()
df = df.withColumn('dedup_hash',
                   hasher(concat(df.first_name, df.last_name))
                   .alias('dedup_hash'))

df_cleansed = df.dropDuplicates(['dedup_hash'])

df_glue_dynamic_frame = DynamicFrame.fromDF(df_cleansed, glueContext, "df_glue_dynamic_frame")

df_write = glueContext.write_dynamic_frame.from_options(
    frame=df_glue_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://glue-learn-begineers/new/",
        "partitionKeys": [],
    },
    transformation_ctx="df_write",
)
