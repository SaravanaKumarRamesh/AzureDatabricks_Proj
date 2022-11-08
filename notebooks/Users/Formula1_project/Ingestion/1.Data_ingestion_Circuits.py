# Databricks notebook source
# DBTITLE 0,### Forumla 1 data Ingestion notebook ###
# MAGIC %md
# MAGIC Ingest data from Azure storage

# COMMAND ----------

# DBTITLE 1,Creating a Schema for the data frame 
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

Circuit_Schema = StructType(fields = [ StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)
    
])


# COMMAND ----------

# DBTITLE 1,Reading the data from the mount
circuits_df = spark.read\
.option("Header",True)\
.schema(Circuit_Schema)\
.csv('/mnt/raw/circuits.csv')

# COMMAND ----------

# DBTITLE 1,removing Columns
from pyspark.sql.functions import col

Circuit_df_select = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))


# COMMAND ----------

# DBTITLE 1,Rename Columns
Circuit_df_rename = Circuit_df_select.withColumnRenamed('circuitId','circuit_Id')\
                                        .withColumnRenamed('circuitRef','circuit_Ref')\
                                        .withColumnRenamed("lat","latitude")\
                                        .withColumnRenamed("lng","Longitude")\
                                        .withColumnRenamed("alt","Altitude")


# COMMAND ----------

# DBTITLE 1,Adding Timestamp column
from pyspark.sql.functions import current_timestamp

Circuit_df_timestamp = Circuit_df_rename.withColumn("Dataingestion_time", current_timestamp())


# COMMAND ----------

# DBTITLE 1,write data to a parquet file
Circuit_df_timestamp.write.mode("overwrite").parquet("dbfs:/mnt/processed/Circuits")
