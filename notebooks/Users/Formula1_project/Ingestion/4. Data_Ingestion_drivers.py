# Databricks notebook source
# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("p_data_source","")
V_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Ingestion from Drivers file #####

# COMMAND ----------

# DBTITLE 1,Functions to be imported
from pyspark.sql.types import IntegerType,StringType,StructType,StructField,DateType
from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

# DBTITLE 1,define a Schema and import the file
Drivers_Schema = StructType(fields = [ StructField ("driverId",IntegerType(),False),
                                        StructField ("driverRef",StringType(),False),
                                        StructField ("number",IntegerType(),True),
                                        StructField ("code",StringType(),False),
                                        StructField ("name",StructType(fields = [ StructField ("forename",StringType(),False),StructField ("surname",StringType(),False)]),False),
                                        StructField ("dob",DateType(),False),
                                        StructField ("nationality",StringType(),False),
                                        StructField ("url",StringType(),False)
                                    
])

Drivers_Df = spark.read\
            .schema(Drivers_Schema)\
            .json(f'/mnt/raw/{V_Filedate}/drivers.json')


# COMMAND ----------

# DBTITLE 1,Transform the Dataframe
Drivers_transform_df = Drivers_Df.withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("driverRef","driver_Ref")\
                                .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
                                .withColumn("Ingestion_time",current_timestamp())\
                                .withColumn("File_date",lit(V_Filedate))\
                                .withColumn("Data_source",lit(V_data_source))\
                                .drop(col("url"))

# COMMAND ----------

# DBTITLE 1,Export it to a parquet file
#Drivers_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/Drivers")

# COMMAND ----------

Drivers_transform_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.Drivers")