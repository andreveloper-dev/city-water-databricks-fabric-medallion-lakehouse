from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def bronze_stream(landing_zone, checkpoint, table):

    schema = StructType() \
        .add("latitude", DoubleType()) \
        .add("longitude", DoubleType()) \
        .add("date", StringType()) \
        .add("customer_id", StringType()) \
        .add("employee_id", StringType()) \
        .add("quantity_products", IntegerType()) \
        .add("order_id", StringType())

    raw_stream_df = (
        spark.readStream
            .format("json")
            .schema(schema)
            .option("path", landing_zone)
            .load()
    )

    clean_stream_df = raw_stream_df.dropDuplicates(["order_id"])
    query = (
        clean_stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(once=True)
            .option("mergeSchema", "true")
            .toTable(table))

    return query


query = bronze_stream(
            landing_zone=landing_zone_path,
            checkpoint=checkpoint_bronze,
            table=Bronze_Orders)