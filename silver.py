## Procesamiento de la capa Silver

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

dyf = glueContext.create_dynamic_frame.from_catalog(database='actividad_3', table_name='raw_events_actividad_3')
dyf.printSchema()

df = dyf.toDF()

# Ejemplo de LootBox ID
loot_box_df = (
    df.where("event.event_type == 'lootbox_opened'")
      .select(
          "event.event_type",
          # Convert Unix epoch timestamp to readable datetime
          from_unixtime((col("event.event_timestamp")).cast("long")).alias("event_datetime"),
          "event.event_data.lootbox_id",
          "event.event_data.lootbox_cost",
          "event.event_data.item_cost",
          col("partition_0").alias("year"),
          col("partition_1").alias("month"),
          col("partition_2").alias("day")
      )
      .dropDuplicates()
      .orderBy(col("event_datetime"))
)


level_df = (
    df.where("event.event_type in ('level_started', 'level_completed', 'level_failed')")
      .select(
          "event.event_type",
          # Convert Unix epoch timestamp to readable datetime
          from_unixtime((col("event.event_timestamp")).cast("long")).alias("event_datetime"),
          "event.event_data.level_id",
          "event.event_data.level_version",
          col("partition_0").alias("year"),
          col("partition_1").alias("month"),
          col("partition_2").alias("day")
      )
      .dropDuplicates()
      .orderBy(col("event_datetime"))
)


iap_df = (
    df.where("event.event_type in ('iap_transaction')")
      .select(
          "event.event_type",
          # Convert Unix epoch timestamp to readable datetime
          from_unixtime((col("event.event_timestamp")).cast("long")).alias("event_datetime"),
          "event.event_data.item_id",
          "event.event_data.item_version",
          "event.event_data.item_amount",
          "event.event_data.currency_type",
          "event.event_data.country_id",
          "event.event_data.currency_amount",
          "event.event_data.transaction_id",
          col("partition_0").alias("year"),
          col("partition_1").alias("month"),
          col("partition_2").alias("day")
      )
      .dropDuplicates()
      .orderBy(col("event_datetime"))
)


match_df = (
    df.where("event.event_type in ('match_start', 'match_end')")
      .select(
          "event.event_type",
          # Convert Unix epoch timestamp to readable datetime
          from_unixtime((col("event.event_timestamp")).cast("long")).alias("event_datetime"),
          "event.event_data.match_id",
          "event.event_data.map_id",
          "event.event_data.match_result_type",
          "event.event_data.exp_gained",
          "event.event_data.most_used_spell",
          col("partition_0").alias("year"),
          col("partition_1").alias("month"),
          col("partition_2").alias("day")
      )
      .dropDuplicates()
      .orderBy(col("event_datetime"))
)
# Save Data 

data_to_save = {
    'lootbox_open': loot_box_df,
    'levels': level_df,
    'match': match_df,
    'iaps': iap_df
}

for name, df in data_to_save.items():

    dyf = DynamicFrame.fromDF(df, glueContext, name)

    s3output = glueContext.getSink(
        path=f"s3://capa-silver-gameanalytics/{name}/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["year", "month", "day"],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="s3output",
    )

    # Update Glue Catalog Table Information
    s3output.setCatalogInfo(
        catalogDatabase="silver", 
        catalogTableName=name
    )

    # Set Output Format
    s3output.setFormat("glueparquet")

    # Write DynamicFrame to S3

    s3output.writeFrame(dyf)
job.commit()
