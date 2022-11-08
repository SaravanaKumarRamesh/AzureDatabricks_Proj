# Databricks notebook source
# DBTITLE 0,### Forumla 1 data Ingestion notebook ###
# MAGIC %md
# MAGIC Ingest data from Azure storage

# COMMAND ----------

# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File , /Users/Saravana_admin@5njbxz.onmicrosoft.com/Utils/Comman_functions

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("p_data_source","")
V_data_source = dbutils.widgets.get("p_data_source")


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
.csv(f'/mnt/raw/{V_Filedate}/circuits.csv')

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
                                        .withColumnRenamed("alt","Altitude")\
                                        .withColumn("File_date",lit(V_Filedate))\
                                        .withColumn("Data_source",lit(V_data_source))

# COMMAND ----------

# DBTITLE 1,Adding Timestamp column
from pyspark.sql.functions import current_timestamp

Circuit_df_timestamp = Circuit_df_rename.withColumn("Dataingestion_time", current_timestamp())


# COMMAND ----------

# DBTITLE 1,write data to a parquet file
#Circuit_df_timestamp.write.mode("overwrite").parquet("dbfs:/mnt/processed/Circuits")

# COMMAND ----------

Circuit_df_timestamp.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_processed.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_processed.circuits retain 0 hours;

# COMMAND ----------

