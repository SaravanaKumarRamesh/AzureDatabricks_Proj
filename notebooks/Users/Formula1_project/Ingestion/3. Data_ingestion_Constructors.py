# Databricks notebook source
# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("p_data_source","")
V_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Ingest Constructors data from Azure Storage ######

# COMMAND ----------

# DBTITLE 1,Create Schema and ingest data into the data frame
from pyspark.sql.types import IntegerType,StringType,StructType,StructField
from pyspark.sql.functions import col,current_timestamp
Constructors_Schema  = StructType(fields = [StructField("constructorId",IntegerType(),False),
                                            StructField("constructorRef",StringType(),False),
                                            StructField("name",StringType(),False),
                                            StructField("nationality",StringType(),False),
                                            StructField("url",StringType(),False)])

Constructor_Df = spark.read.schema(Constructors_Schema).json(f'/mnt/raw/{V_Filedate}/constructors.json')

# COMMAND ----------

# DBTITLE 1,Rename Columns and remove columns
Constructor_renamedf = Constructor_Df.withColumnRenamed("constructorId","constructor_Id")\
                                        .withColumnRenamed("constructorRef","constructor_Ref")\
                                        .withColumn("Ingestion_time",current_timestamp())\
                                        .withColumn("File_date",lit(V_Filedate))\
                                        .withColumn("Data_source",lit(V_data_source))\
                                        .drop(col("url"))

# COMMAND ----------

#Constructor_renamedf.write.mode("overwrite").parquet("dbfs:/mnt/processed/Constructors")

# COMMAND ----------

Constructor_renamedf.write.mode("overwrite").format("delta").saveAsTable("f1_processed.Constructors")