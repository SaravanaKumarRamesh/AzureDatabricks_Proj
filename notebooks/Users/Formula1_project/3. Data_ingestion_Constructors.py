# Databricks notebook source
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

Constructor_Df = spark.read.schema(Constructors_Schema).json("/mnt/raw/constructors.json")

# COMMAND ----------

# DBTITLE 1,Rename Columns and remove columns
Constructor_renamedf = Constructor_Df.withColumnRenamed("constructorId","constructor_Id")\
                                        .withColumnRenamed("constructorRef","constructor_Ref")\
                                        .withColumn("Ingestion_time",current_timestamp())\
                                        .drop(col("url"))

# COMMAND ----------

Constructor_renamedf.write.mode("overwrite").parquet("dbfs:/mnt/processed/Constructors")

# COMMAND ----------

