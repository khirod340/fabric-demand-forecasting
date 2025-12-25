# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6ed1356b-74ef-412f-b7c0-6ca28b7376ca",
# META       "default_lakehouse_name": "sales_lakehouse",
# META       "default_lakehouse_workspace_id": "30ba803b-3d2f-4d3c-8816-9fceb2041c9d",
# META       "known_lakehouses": [
# META         {
# META           "id": "6ed1356b-74ef-412f-b7c0-6ca28b7376ca"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import mlflow
import mlflow.sklearn
from pyspark.sql.functions import current_timestamp
import pandas as pd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.set_experiment("sales_forecasting_experiment")

runs = mlflow.search_runs(
    order_by=["start_time DESC"],
    max_results=1
)

run_id = runs.iloc[0]["run_id"]
print("Using run_id:", run_id)

model_uri = f"runs:/{run_id}/model"
model = mlflow.sklearn.load_model(model_uri)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = spark.table("gold_sales_features")

latest_date = df_gold.selectExpr("max(date) as max_date").collect()[0]["max_date"]
print("Forecasting for date:", latest_date)

predict_df = df_gold.filter(df_gold.date == latest_date)

display(predict_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FEATURES = [
    "price",
    "promotion_flag",
    "lag_7_units",
    "rolling_7_avg",
    "day_of_week",
    "month"
]

predict_pd = predict_df.toPandas()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predict_pd["predicted_units"] = model.predict(
    predict_pd[FEATURES]
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predict_pd["model_run_id"] = run_id
predict_pd["prediction_timestamp"] = pd.Timestamp.now()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

forecast_pd = predict_pd[
    ["date", "product", "region", "predicted_units",
     "model_run_id", "prediction_timestamp"]
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

forecast_df = spark.createDataFrame(forecast_pd)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    forecast_df
    .write
    .mode("append")
    .format("delta")
    .saveAsTable("gold_sales_forecast")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("gold_sales_forecast"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
