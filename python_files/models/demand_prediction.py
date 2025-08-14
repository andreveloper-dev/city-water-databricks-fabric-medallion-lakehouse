import mlflow.pyfunc
from pyspark.ml import PipelineModel
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, lit

training_dataset = spark.table(Gold_Train_Model_Dataset)
tomorrow = datetime.now(horario_colombia) + timedelta(days=1)
day_number_tomorrow = (tomorrow.weekday() + 2)
if day_number_tomorrow > 7:
    day_number_tomorrow -= 7
window = Window.partitionBy("neighborhood").orderBy(col("event_date").desc())
df_latest = training_dataset.withColumn("row_num", row_number().over(window))\
                            .filter(col("row_num") == 1)\
                            .drop("row_num")
df_pred = df_latest.withColumn("day_number", lit(day_number_tomorrow))\
                   .drop(*["event_date", "quantity_products"])
model_uri = "/Volumes/unalwater_v2/default/files/Model"
model = mlflow.spark.load_model(model_uri)
df_result  = model.transform(df_pred)
predictions = model.transform(training_dataset)
bias_by_neighborhood = calculate_bias_by_neighborhood(dataset=predictions)
df_result_with_bias = df_result.join(
    bias_by_neighborhood.select("neighborhood", "avg_bias", "bias_percentage"),
    on="neighborhood",
    how="left"
)
df_final = df_result_with_bias.select(
    "neighborhood",
    "prediction",
    "avg_bias",
    "bias_percentage"
)
df_final.write.format('delta').mode("overwrite").save(Gold_Demand_Prediction)
df_final.write.mode("overwrite").saveAsTable("gold_demand_prediction")