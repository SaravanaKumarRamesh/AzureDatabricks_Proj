# Databricks notebook source
# MAGIC %sql
# MAGIC Create database if not exists f1_presentation
# MAGIC location '/mnt/presentation'

# COMMAND ----------

# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

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
Races_df = spark.read.option("Header",True).schema(Races_Schema).csv(f"{raw_folder_path}/races.csv")\
            .withColumnRenamed("year","Race_year")\
            .withColumnRenamed("name","Race_name")\
            .withColumnRenamed("date","Race_date")

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
Results_df = spark.read.schema(Results_schema).json(f"{raw_folder_path}/results.json").withColumnRenamed("time","Race_time")

Circuit_Schema = StructType(fields = [ StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)])
circuits_df = spark.read.option("Header",True).schema(Circuit_Schema).csv(f"{raw_folder_path}/circuits.csv").withColumnRenamed("location","Circuit_location")

Drivers_Schema = StructType(fields = [ StructField ("driverId",IntegerType(),False),
                                        StructField ("driverRef",StringType(),False),
                                        StructField ("number",IntegerType(),True),
                                        StructField ("code",StringType(),False),
                                        StructField ("name",StructType(fields = [ StructField ("forename",StringType(),False),StructField ("surname",StringType(),False)]),False),
                                        StructField ("dob",DateType(),False),
                                        StructField ("nationality",StringType(),False),
                                        StructField ("url",StringType(),False)])
Drivers_Df = spark.read.schema(Drivers_Schema).json(f"{raw_folder_path}/drivers.json")\
            .withColumn("Driver_name",concat(col("name.forename"), lit(" "), col("name.surname")))\
            .withColumnRenamed("number","Driver_number")\
            .withColumnRenamed("nationality","Driver_nationality")

Constructors_Schema  = StructType(fields = [StructField("constructorId",IntegerType(),False),
                                            StructField("constructorRef",StringType(),False),
                                            StructField("name",StringType(),False),
                                            StructField("nationality",StringType(),False),
                                            StructField("url",StringType(),False)])

Constructor_Df = spark.read.schema(Constructors_Schema).json(f"{raw_folder_path}/constructors.json").withColumnRenamed("name","Team")

RaCi_df = Races_df.join(circuits_df,Races_df.circuitId ==circuits_df.circuitId,"leftouter")

RedrCon_df =(Results_df.join(Drivers_Df,Results_df.driverId ==Drivers_Df.driverId,"leftouter")\
                          .join(Constructor_Df,Results_df.constructorId ==Constructor_Df.constructorId,"leftouter"))

Presentation_Out = RaCi_df.join(RedrCon_df,RaCi_df.raceId == RedrCon_df.raceId,"inner")\
                            .select(RaCi_df.Race_year,RaCi_df.Race_name,RaCi_df.Race_date,RaCi_df.Circuit_location\
                                    ,RedrCon_df.Driver_name,RedrCon_df.Driver_number,RedrCon_df.Driver_nationality,RedrCon_df.Team\
                                    ,RedrCon_df.position,RedrCon_df.grid,RedrCon_df.fastestLap,RedrCon_df.Race_time,RedrCon_df.points)\
                                    .withColumn("Created_date",current_timestamp())
#Presentation_Out.write.mode("overwrite").parquet(f"dbfs:{Presentation_folder_path}/Presentation")


Presentation_Out.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")


# COMMAND ----------

presentation_mod_df = Presentation_Out.select("Race_year","Driver_name","Race_name","points")
window_mod = presentation_mod_df.groupBy("Race_year","Driver_name")\
.agg(sum("points").alias("points"),countDistinct("Race_name"))\
.orderBy(desc("points"))
winspec = Window.partitionBy("Race_year").orderBy(desc("points"))
Driver_standings_df = window_mod.withColumn("rank",dense_rank().over(winspec))
Driver_standings_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.Driver_standings")

# COMMAND ----------

Constructors_df = Presentation_Out.groupBy("Race_year","team").agg(sum("points").alias("points"))
Cons_window_Spec  = Window.partitionBy("Race_year").orderBy(desc("points"))
Constructors_final = Constructors_df.withColumn("Rank", rank().over(Cons_window_Spec))
Constructors_final.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.Constructors_standings")