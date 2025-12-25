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

import pandas as pd
from pyspark.sql.functions import max as spark_max


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold_sales_features").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import min, max

spark.table("gold_sales_features").select(
    min("date").alias("min_date"),
    max("date").alias("max_date")
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql.functions import max as spark_max

df_gold = spark.table("gold_sales_features")

max_date = df_gold.select(spark_max("date")).collect()[0][0]
cutoff_date = max_date - pd.DateOffset(days=2)

print("Max date:", max_date)
print("Cutoff date:", cutoff_date)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_df = df_gold.filter(df_gold.date < cutoff_date)
test_df  = df_gold.filter(df_gold.date >= cutoff_date)

print("Train rows:", train_df.count())
print("Test rows:", test_df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = spark.table("gold_sales_features")

display(df_gold)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_pd = train_df.toPandas()
test_pd  = test_df.toPandas()


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

TARGET = "units_sold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if train_df.count() == 0:
    raise Exception(
        "Cold start detected: no historical data available for training."
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

model = LinearRegression()
model.fit(train_pd[FEATURES], train_pd[TARGET])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

preds = model.predict(test_pd[FEATURES])

mae = mean_absolute_error(test_pd[TARGET], preds)
rmse = np.sqrt(mean_squared_error(test_pd[TARGET], preds))

print("MAE:", mae)
print("RMSE:", rmse)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.set_experiment("sales_forecasting_experiment")

with mlflow.start_run():
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("features", ",".join(FEATURES))
    mlflow.log_param("train_rows", len(train_pd))
    mlflow.log_param("test_rows", len(test_pd))
    mlflow.log_param("cutoff_date", str(cutoff_date))

    mlflow.log_metric("mae", mae)
    mlflow.log_metric("rmse", rmse)

    mlflow.sklearn.log_model(model, "model")


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
