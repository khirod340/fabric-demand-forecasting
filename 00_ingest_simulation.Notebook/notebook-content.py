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

from pyspark.sql.functions import current_timestamp

# read raw csv file

file_path = "Files/incoming/"

df_raw = (
    spark.read
    .option("header", True)
    .csv(file_path)
)


display(df_raw)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# add time stamp
df_bronze = df_raw.withColumn("ingestion_timestamp",current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_bronze
    .write
    .mode("append")
    .format("delta")
    .saveAsTable("bronze_sales")
)


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
