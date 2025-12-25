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

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, avg, dayofweek, month

df_silver = spark.table("silver_sales")

display(df_silver)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = (
    Window
    .partitionBy("product", "region")
    .orderBy("date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = (
    df_silver
    .withColumn("lag_7_units", lag("units_sold", 7).over(window_spec))
    .withColumn("lag_30_units", lag("units_sold", 30).over(window_spec))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rolling_7 = window_spec.rowsBetween(-7, -1)
rolling_30 = window_spec.rowsBetween(-30, -1)

df_gold = (
    df_gold
    .withColumn("rolling_7_avg", avg("units_sold").over(rolling_7))
    .withColumn("rolling_30_avg", avg("units_sold").over(rolling_30))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = (
    df_gold
    .withColumn("day_of_week", dayofweek("date"))
    .withColumn("month", month("date"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = df_gold.dropna(
    subset=["lag_7_units", "rolling_7_avg"]
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_gold
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold_sales_features")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("gold_sales_features"))


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
