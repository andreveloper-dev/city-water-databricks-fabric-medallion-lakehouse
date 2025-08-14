import mlflow
import mlflow.spark
from pyspark.ml.feature import StringIndexer, OneHotEncoder, StandardScaler, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from mlflow.models.signature import infer_signature

df = spark.table(Gold_Train_Model_Dataset).drop("event_date")
train, test = df.randomSplit([0.8, 0.2], seed=666)
neighborhood_indexer = StringIndexer(inputCol="neighborhood", outputCol="neighborhood_index", handleInvalid="keep")

day_indexer = StringIndexer(inputCol="day_number", outputCol="day_number_index", handleInvalid="keep")
neighborhood_encoder = OneHotEncoder(inputCol="neighborhood_index", outputCol="neighborhood_ohe")
day_encoder = OneHotEncoder(inputCol="day_number_index", outputCol="day_number_ohe")

numeric_cols = [col for col in train.columns if col not in ["neighborhood", "day_number", "quantity_products", "event_date"]]
vec_numeric = VectorAssembler(inputCols=numeric_cols, outputCol="numeric_features_vec")
scaler = StandardScaler(inputCol="numeric_features_vec", outputCol="numeric_features_scaled")
assembler = VectorAssembler(
    inputCols=["numeric_features_scaled", "neighborhood_ohe", "day_number_ohe"],
    outputCol="features"
)
models = {
    "LinearRegression": LinearRegression(featuresCol="features", labelCol="quantity_products"),
    "RandomForest": RandomForestRegressor(featuresCol="features", labelCol="quantity_products", seed=666),
    "GradientBoostedTrees": GBTRegressor(featuresCol="features", labelCol="quantity_products", seed=666)
}
evaluators = {
    "RMSE": RegressionEvaluator(labelCol="quantity_products", predictionCol="prediction", metricName="rmse"),
    "MAE": RegressionEvaluator(labelCol="quantity_products", predictionCol="prediction", metricName="mae"),
    "R2": RegressionEvaluator(labelCol="quantity_products", predictionCol="prediction", metricName="r2")
}

results = {}
trained_models = {}

for model_name, model in models.items():
    print(f"\nEntrenando {model_name}")
    pipeline = Pipeline(stages=[
        neighborhood_indexer,
        day_indexer,
        neighborhood_encoder,
        day_encoder,
        vec_numeric,
        scaler,
        assembler,
        model]) 
    pipeline_model = pipeline.fit(train)
    trained_models[model_name] = pipeline_model
    predictions = pipeline_model.transform(test)
    metrics = {}
    for metric_name, evaluator in evaluators.items():
        metrics[metric_name] = evaluator.evaluate(predictions)
    results[model_name] = metrics
    predictions_train = pipeline_model.transform(train)
    signature = infer_signature(train.drop("quantity_products"), predictions_train.select("prediction"))
    with mlflow.start_run(run_name=f"{model_name}_run"):
        mlflow.spark.log_model(
            pipeline_model,
            artifact_path="model",
            registered_model_name=f"{model_name}_v2",
            signature=signature
        )
print(f"{'Modelo':<20} {'RMSE':<10} {'MAE':<10} {'R2':<10}")
for model_name, metrics in results.items():
    print(f"{model_name:<20} {metrics['RMSE']:<10.2f} {metrics['MAE']:<10.2f} "
          f"{metrics['R2']:<10.4f}")

sorted_models = sorted(results.items(), key=lambda item: item[1]['RMSE'])
best_model_name = sorted_models[0][0]
best_model = trained_models[best_model_name]

print(f"\nMEJOR MODELO: {best_model_name}")
best_predictions = best_model.transform(df)
bias_by_neighborhood = calculate_bias_by_neighborhood(dataset=best_predictions)
mlflow.spark.save_model(
    spark_model=best_model,
    path="/Volumes/unalwater_v2/default/files/model_test/"
)
