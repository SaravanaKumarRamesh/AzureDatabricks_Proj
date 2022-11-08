# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Ingestion of laptimes #####

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("P_source","Manual")
V_Source = dbutils.widgets.get("P_source")

# COMMAND ----------

# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

laptimes_Schema = StructType(fields = [ StructField ("qualifyId",IntegerType(),False),
                                        StructField ("driverId",IntegerType(),False),
                                        StructField ("lap",IntegerType(),False),
                                        StructField ("position",IntegerType(),False),
                                        StructField ("time",StringType(),False),
                                        StructField ("milliseconds",IntegerType(),False)
                                    
])

laptimes_Df = spark.read\
            .schema(laptimes_Schema)\
            .csv(f"{raw_folder_path}/{V_Filedate}/lap_times")

# COMMAND ----------

Laptimes_transform_df = laptimes_Df.withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumn("Field_date",lit(V_Filedate))\
                                .withColumn("Source",lit(V_Source))\
                                .withColumn("Ingestion_time",current_timestamp())

# COMMAND ----------

#Laptimes_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/lap_times")

# COMMAND ----------

Merge_condition = "tgt.driver_Id= src.driver_Id"
merge_delta_data(Laptimes_transform_df,'f1_processed','lap_times',processed_folder_path,Merge_condition,'driver_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.lap_times
# MAGIC Group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

display (laptimes_Df)

# COMMAND ----------

