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
import pandas as pd

mlflow.set_experiment("sales_forecasting_experiment")

runs = mlflow.search_runs(
    order_by=["start_time DESC"]
)

runs.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

perf_pd = runs[[
    "run_id",
    "start_time",
    "metrics.mae",
    "metrics.rmse",
    "params.model_type",
    "params.train_rows",
    "params.test_rows"
]].copy()

perf_pd.columns = [
    "run_id",
    "run_timestamp",
    "mae",
    "rmse",
    "model_type",
    "train_rows",
    "test_rows"
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

forecast_df = spark.table("gold_sales_forecast")

display(forecast_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

perf_df = spark.createDataFrame(perf_pd)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    perf_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("model_performance_metrics")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import avg, count

forecast_health = (
    forecast_df
    .groupBy("model_run_id")
    .agg(
        count("*").alias("forecast_rows"),
        avg("predicted_units").alias("avg_prediction")
    )
)

display(forecast_health)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    forecast_health
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("forecast_health_metrics")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

perf_pd = perf_df.orderBy("run_timestamp").toPandas()

perf_pd["mae_change_pct"] = perf_pd["mae"].pct_change()

alerts = perf_pd[
    perf_pd["mae_change_pct"] > 0.25
].copy()

alerts["alert_type"] = "MODEL_PERFORMANCE_DEGRADATION"
alerts["severity"] = "HIGH"
alerts["alert_message"] = "MAE increased more than 25% vs previous run"
alerts["alert_timestamp"] = pd.Timestamp.now()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

forecast_pd = forecast_health.toPandas()

forecast_pd["alert_flag"] = forecast_pd["forecast_rows"] == 0

forecast_alerts = forecast_pd[forecast_pd["alert_flag"]].copy()

forecast_alerts["alert_type"] = "FORECAST_EMPTY"
forecast_alerts["severity"] = "CRITICAL"
forecast_alerts["alert_message"] = "No forecasts generated for run"
forecast_alerts["alert_timestamp"] = pd.Timestamp.now()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

alerts_pd = pd.concat([
    alerts[[
        "run_id",
        "alert_type",
        "severity",
        "alert_message",
        "alert_timestamp"
    ]],
    forecast_alerts.rename(columns={"model_run_id": "run_id"})[[
        "run_id",
        "alert_type",
        "severity",
        "alert_message",
        "alert_timestamp"
    ]]
], ignore_index=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)

alert_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("alert_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("alert_message", StringType(), True),
    StructField("alert_timestamp", TimestampType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

alerts_df = spark.createDataFrame(alerts_pd, schema=alert_schema)


(
    alerts_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_alerts")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    alerts_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_alerts")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("model_performance_metrics"))
display(spark.table("forecast_health_metrics"))
display(spark.table("ml_alerts"))


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
