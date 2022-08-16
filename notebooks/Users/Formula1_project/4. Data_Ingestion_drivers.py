# Databricks notebook source
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
            .json("/mnt/raw/drivers.json")


# COMMAND ----------

# DBTITLE 1,Transform the Dataframe
Drivers_transform_df = Drivers_Df.withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("driverRef","driver_Ref")\
                                .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
                                .withColumn("Ingestion_time",current_timestamp())\
                                .drop(col("url"))

# COMMAND ----------

# DBTITLE 1,Export it to a parquet file
Drivers_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/Drivers")

# COMMAND ----------

display(Drivers_transform_df)

# COMMAND ----------

