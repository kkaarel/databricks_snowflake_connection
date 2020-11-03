# Databricks notebook source


# Create key vault connection: Go to https://<databricks-instance>#secrets/createScope
# There might be that you need to add Access Polices in key vault  for Databricks. Application needs secret premissions. "Get" and "List"
user = dbutils.secrets.get("databrickskv", "sfuser")
password = dbutils.secrets.get("databrickskv", "sfpassword")

# Sll these are mandatory fields!
options = {
  "sfUrl": "<your snowflake ur(no need for http://)>.snowflakecomputing.com/",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "<databasename>",
  "sfSchema": "<schema>",
  "sfWarehouse": "<warehouse>",
  "sfRole": "<role>"
}

# COMMAND ----------

df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query",  "select * from <tablename>") \
  .load()

df.show()
