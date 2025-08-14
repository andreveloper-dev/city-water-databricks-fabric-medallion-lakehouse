import geopandas as gpd
import pytz
from shapely import wkb
from pyspark.sql.functions import col, avg, round

def spark_to_geopandas(df_spark, crs="EPSG:4326"):
    df = df_spark.toPandas()
    if isinstance(df['geometry'].iloc[0], bytes):
        df['geometry'] = df['geometry'].apply(wkb.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    if gdf.crs is None:
        gdf.set_crs(crs, inplace=True)   
    return gdf

horario_colombia = pytz.timezone("America/Bogota")
def calculate_bias_by_neighborhood(dataset):
    bias_by_neighborhood = dataset.groupBy("neighborhood").agg(
        avg("quantity_products").alias("avg_real"),
        avg("prediction").alias("avg_predicted")
    ).withColumn(
        "avg_bias", col("avg_predicted") - col("avg_real")
    ).withColumn(
        "bias_percentage", round((col("avg_bias") / col("avg_real")), 3)
    ).orderBy("neighborhood")

    return bias_by_neighborhood