# Databricks notebook source
# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("p_data_source","")
V_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# DBTITLE 1,Create a Schema and import Races.csv
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

Races_Schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                    StructField("year",IntegerType(),False),
                                    StructField("round",IntegerType(),True),
                                    StructField("circuitId",IntegerType(),True),
                                    StructField("name",StringType(),False),
                                    StructField("date",StringType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("url",StringType(),True)  
                                    ])

Races_df = spark.read.option("Header",True).schema(Races_Schema).csv(f'/mnt/raw/{V_Filedate}/races.csv')

# COMMAND ----------

# DBTITLE 1,Select and rename the columns
from pyspark.sql.functions import col
Races_df_renamed = Races_df.select(col("raceId").alias("race_Id"),col("year").alias("Race_year"),col("round"),col("circuitId").alias("circuit_Id"),col("name"),col("date"),col("time"))

# COMMAND ----------

# DBTITLE 1,Adding Timestamp and ingestion date
from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit

Races_df_result = Races_df_renamed.withColumn("Dataingestion_time", current_timestamp())\
                                    .withColumn("Race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss'))\
                                    .withColumn("File_date",lit(V_Filedate))\
                                    .withColumn("Data_source",lit(V_data_source))
                

# COMMAND ----------

# DBTITLE 1,Write the Output to parquet file
#Races_df_result.write.mode("overwrite").parquet("dbfs:/mnt/processed/races")

# COMMAND ----------

Races_df_result.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")