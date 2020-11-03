// Databricks notebook source
import org.apache.spark.sql.DataFrame


%scala
// Use secrets DBUtil to get Snowflake credentials.
// Create key vault connection: Go to https://<databricks-instance>#secrets/createScope
// There might be that you need to add Access Polices in key vault  for Databricks. Application needs secret premissions. "Get" and "List"
val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
val user = dbutils.secrets.get("databrickskv", "sfuser")
val password = dbutils.secrets.get("databrickskv", "sfpassword")

// Sll these are mandatory fields!

val options = Map(
  "sfUrl" -> "<yoursnowflake ur(no need for http://)>.snowflakecomputing.com/",
  "sfUser" -> user,
  "sfPassword" -> password,
  "sfDatabase" -> "<databasename>",
  "sfSchema" -> "<schema>",
  "sfWarehouse" -> "<warehouse>",
  "sfRole" -> "<role>"
)

// COMMAND ----------

val df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "<table>")
  .load()

display(df)

// COMMAND ----------

val df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("query", "select * from <table>;")
  .load()

display(df)

// COMMAND ----------
