# Databricks notebook source
# MAGIC %sql
# MAGIC Create database if not exists f1_processed
# MAGIC location '/mnt/processed'

# COMMAND ----------

# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

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

circuits_df = spark.read\
.option("Header",True)\
.schema(Circuit_Schema)\
.csv('/mnt/raw/circuits.csv')

Circuit_df_select = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

Circuit_df_rename = Circuit_df_select.withColumnRenamed('circuitId','circuit_Id')\
                                        .withColumnRenamed('circuitRef','circuit_Ref')\
                                        .withColumnRenamed("lat","latitude")\
                                        .withColumnRenamed("lng","Longitude")\
                                        .withColumnRenamed("alt","Altitude")

from pyspark.sql.functions import current_timestamp

Circuit_df_timestamp = Circuit_df_rename.withColumn("Dataingestion_time", current_timestamp())

#Circuit_df_timestamp.write.mode("overwrite").parquet("dbfs:/mnt/processed/Circuits")

Circuit_df_timestamp.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

Races_Schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                    StructField("year",IntegerType(),False),
                                    StructField("round",IntegerType(),True),
                                    StructField("circuitId",IntegerType(),True),
                                    StructField("name",StringType(),False),
                                    StructField("date",StringType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("url",StringType(),True)  
                                    ])

Races_df = spark.read.option("Header",True).schema(Races_Schema).csv('/mnt/raw/races.csv')
from pyspark.sql.functions import col
Races_df_renamed = Races_df.select(col("raceId").alias("race_Id"),col("year").alias("Race_year"),col("round"),col("circuitId").alias("circuit_Id"),col("name"),col("date"),col("time"))
from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit

Races_df_result = Races_df_renamed.withColumn("Dataingestion_time", current_timestamp())\
                                    .withColumn("Race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss'))
                
#Races_df_result.write.mode("overwrite").parquet("dbfs:/mnt/processed/races")
Races_df_result.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")


# COMMAND ----------

Constructors_Schema  = StructType(fields = [StructField("constructorId",IntegerType(),False),
                                            StructField("constructorRef",StringType(),False),
                                            StructField("name",StringType(),False),
                                            StructField("nationality",StringType(),False),
                                            StructField("url",StringType(),False)])

Constructor_Df = spark.read.schema(Constructors_Schema).json("/mnt/raw/constructors.json")
Constructor_renamedf = Constructor_Df.withColumnRenamed("constructorId","constructor_Id")\
                                        .withColumnRenamed("constructorRef","constructor_Ref")\
                                        .withColumn("Ingestion_time",current_timestamp())\
                                        .drop(col("url"))
#Constructor_renamedf.write.mode("overwrite").parquet("dbfs:/mnt/processed/Constructors")
Constructor_renamedf.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.Constructors")

# COMMAND ----------

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

Drivers_transform_df = Drivers_Df.withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("driverRef","driver_Ref")\
                                .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
                                .withColumn("Ingestion_time",current_timestamp())\
                                .drop(col("url"))
#Drivers_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/Drivers")

Drivers_transform_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.Drivers")

# COMMAND ----------

Results_schema = StructType (fields = [StructField("resultId",IntegerType(),True),
                                        StructField("raceId",IntegerType(),True),
                                        StructField("driverId",IntegerType(),True),
                                        StructField("constructorId",IntegerType(),True),
                                        StructField("number",IntegerType(),True),
                                        StructField("grid",IntegerType(),True),
                                        StructField("position",IntegerType(),True),
                                        StructField("positionText",StringType(),True),
                                        StructField("positionOrder",IntegerType(),True),
                                        StructField("points",FloatType(),True),
                                        StructField("laps",IntegerType(),True),
                                        StructField("time",StringType(),True),
                                        StructField("milliseconds",IntegerType(),True),
                                        StructField("fastestLap",IntegerType(),True),
                                        StructField("rank",IntegerType(),True),
                                        StructField("fastestLapTime",StringType(),True),
                                        StructField("fastestLapSpeed",FloatType(),True),
                                        StructField("statusId",IntegerType(),True)
                                        ])

Results_df = spark.read\
         .schema(Results_schema)\
         .json("/mnt/raw/results.json")

Results_Transform_df = Results_df.withColumnRenamed("resultId","result_Id")\
                                .withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("constructorId","constructor_Id")\
                                .withColumnRenamed("positionText","position_Text")\
                                .withColumnRenamed("positionOrder","position_Order")\
                                .withColumnRenamed("fastestLap","fastest_Lap")\
                                .withColumnRenamed("fastestLapTime","fastest_Lap_Time")\
                                .withColumnRenamed("fastestLapSpeed","fastest_Lap_Speed")\
                                .withColumn("Ingestion_time",current_timestamp())\
                                .drop(col("statusId"))

#Results_Transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/results")

Results_Transform_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.Results")


# COMMAND ----------

Pitstops_Schema = StructType(fields = [ StructField ("raceId",IntegerType(),False),
                                        StructField ("driverId",StringType(),False),
                                        StructField ("stop",IntegerType(),True),
                                        StructField ("lap",IntegerType(),False),
                                        StructField ("time",StringType(),False),
                                        StructField ("duration",FloatType(),False),
                                        StructField ("milliseconds",IntegerType(),False)
                                    
])

Pitstops_Df = spark.read\
            .schema(Pitstops_Schema)\
            .option("multiline", True)\
            .json("/mnt/raw/pit_stops.json")
Pitstops_transform_df = Pitstops_Df.withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumn("Ingestion_time",current_timestamp())
#Pitstops_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/pit_stops")
Pitstops_transform_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.Pitstops")


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
            .json("/mnt/raw/lap_times")
Laptimes_transform_df = laptimes_Df.withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumn("Ingestion_time",current_timestamp())
#Laptimes_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/lap_times")
Laptimes_transform_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.Laptimes")


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
            .json("/mnt/raw/qualifying")
Qualifying_transform_df = Qualifying_Df.withColumnRenamed("raceId","race_Id")\
                                        .withColumnRenamed("qualifyId","qualify_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("constructorId","constructor_Id")\
                                .withColumn("Ingestion_time",current_timestamp())
#Qualifying_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/Qualifying")
Qualifying_transform_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.Qualifying")

# COMMAND ----------

