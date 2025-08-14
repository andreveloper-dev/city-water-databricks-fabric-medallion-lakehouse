from datetime import datetime
from pyspark.sql.functions import to_timestamp, col, min, max, count, sum as _sum, lit, datediff
import pytz

horario_colombia = pytz.timezone("America/Bogota")
current_date = datetime.now(horario_colombia).strftime("%d/%m/%Y %H:%M:%S")
employees = spark.table(Bronze_Employes)
orders = spark.table(Silver_Orders)

def generate_silver_employees(orders_df, employees_df, table_silver, current_date):
    clean_orders_df = orders_df.dropDuplicates(["order_id"])
    clean_orders_df = clean_orders_df.withColumn("approved_date", to_timestamp(col("event_date"), "dd/MM/yyyy HH:mm:ss"))
    orders_agg = clean_orders_df.groupBy("employee_id").agg(
    min("approved_date").alias("first_sale"),
    max("approved_date").alias("last_sale"),
    count("order_id").alias("total_orders"),
    _sum("quantity_products").alias("total_products"),
    min("quantity_products").alias("min_quantity_sold"),
    max("quantity_products").alias("max_quantity_sold"))
    orders_agg = orders_agg.withColumn("load_date", to_timestamp(lit(current_date), "dd/MM/yyyy HH:mm:ss"))
    orders_agg = orders_agg.withColumn("days_since_first_sale", datediff(col("load_date"), col("first_sale")))
    df_merged = employees_df.join(
        orders_agg,
        on="employee_id",
        how="left")
    df_merged.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_silver)
    return df_merged

generate_silver_employees(
    orders_df=orders,
    employees_df=employees,
    table_silver=Silver_Employees,
    current_date=current_date
)