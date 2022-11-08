# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Ingestion of Qualifying #####

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("P_source","Manual")
V_Source = dbutils.widgets.get("P_source")

# COMMAND ----------

V_Filedate

# COMMAND ----------

# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

Qualifying_Schema = StructType(fields = [ StructField ("qualifyId",IntegerType(),False),
                                        StructField ("raceId",IntegerType(),False),
                                        StructField ("driverId",IntegerType(),False),
                                        StructField ("constructorId",IntegerType(),False),
                                        StructField ("number",IntegerType(),True),
                                        StructField ("position",IntegerType(),False),
                                        StructField ("q1",StringType(),False),
                                        StructField ("q2",StringType(),False),
                                        StructField ("q3",StringType(),False)
                                    
])

Qualifying_Df = spark.read\
            .schema(Qualifying_Schema)\
            .option("multiline", True)\
            .json(f"{raw_folder_path}/{V_Filedate}/qualifying")

# COMMAND ----------

Qualifying_transform_df = Qualifying_Df.withColumnRenamed("raceId","race_Id")\
                                        .withColumnRenamed("qualifyId","qualify_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("constructorId","constructor_Id")\
                                .withColumn("Field_date",lit(V_Filedate))\
                                .withColumn("Source",lit(V_Source))\
                                .withColumn("Ingestion_time",current_timestamp())

# COMMAND ----------

#Qualifying_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/Qualifying")

# COMMAND ----------

Merge_condition = "tgt.driver_Id= src.driver_Id"
merge_delta_data(Qualifying_transform_df,'f1_processed','qualifying',processed_folder_path,Merge_condition,'driver_Id')

# COMMAND ----------

display(Qualifying_transform_df)