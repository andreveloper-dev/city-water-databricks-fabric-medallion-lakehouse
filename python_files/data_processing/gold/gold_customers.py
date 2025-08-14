from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp, row_number, sum as _sum, dense_rank

orders = spark.table(Silver_Orders)
geodata = spark.table(Bronze_Geodata)
geodata = geodata.select(
    col('IDENTIFICACION').alias('district'),
    col('NOMBRE').alias('neighborhood'),
    'geometry'
)
display(geodata)

def customers_location(df_orders, geodata, table_gold):
    orders_filtered = df_orders.select('district',
                                    'neighborhood',
                                    'quantity_products')
    orders_grouped = orders_filtered.groupBy("district", "neighborhood").agg(
    _sum("quantity_products").alias("total_products"))
    window_spec = Window.orderBy(col("total_products").desc())
    orders_filtered_ranked = orders_grouped.withColumn("ranking", dense_rank().over(window_spec))
    orders_with_geom = orders_filtered_ranked.join(
    geodata,
    on=["district", "neighborhood"],
    how="left"
    )
    gdf_orders = spark_to_geopandas(orders_with_geom)
    gdf_orders = gdf_orders.set_crs("EPSG:3116", allow_override=True)
    gdf_orders["centroid"] = gdf_orders["geometry"].centroid
    gdf_orders = gdf_orders.to_crs("EPSG:4326")
    gdf_orders["longitude"] = gdf_orders["centroid"].x
    gdf_orders["latitude"] = gdf_orders["centroid"].y
    orders_enriched = spark.createDataFrame(gdf_orders.drop(columns=["geometry", "centroid"]))
    orders_enriched = orders_enriched.orderBy(col("ranking").asc())
    orders_enriched.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_gold)

    return orders_enriched


customers_location(
    df_orders=orders,
    geodata=geodata,
    table_gold=Gold_Location_Customers
)
orders = spark.table(Silver_Orders)
print(orders.head())
geodata = spark.table(Bronze_Geodata)
print(geodata.head())
geodata = geodata.select(
    col('IDENTIFICACION').alias('district'),
    col('NOMBRE').alias('neighborhood'),
    'geometry'
)
print(geodata.head())
df_orders = orders
geodata = geodata
table_gold = Gold_Location_Customers
orders_filtered = df_orders.select('district',
                                    'neighborhood',
                                    'quantity_products')
orders_grouped = orders_filtered.groupBy("district", "neighborhood").agg(_sum("quantity_products").alias("total_products"))
window_spec = Window.orderBy(col("total_products").desc())
orders_filtered_ranked = orders_grouped.withColumn("ranking", dense_rank().over(window_spec))
orders_with_geom = orders_filtered_ranked.join(
    geodata,
    on=["district", "neighborhood"],
    how="left"
    )    
gdf_orders = spark_to_geopandas(orders_with_geom)
gdf_orders = gdf_orders.set_crs("EPSG:3116", allow_override=True)
gdf_orders["centroid"] = gdf_orders["geometry"].centroid
gdf_orders = gdf_orders.to_crs("EPSG:4326")
gdf_orders["longitude"] = gdf_orders["centroid"].x
gdf_orders["latitude"] = gdf_orders["centroid"].y
orders_enriched = spark.createDataFrame(gdf_orders.drop(columns=["geometry", "centroid"]))
orders_enriched = orders_enriched.orderBy(col("ranking").asc())
orders_enriched.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(Gold_Location_Customers)