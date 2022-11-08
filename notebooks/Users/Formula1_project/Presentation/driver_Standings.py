# Databricks notebook source
# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

presentation_df = spark.read.parquet("/mnt/presentation/Presentation")

# COMMAND ----------

presentation_df = spark.read.parquet("/mnt/presentation/race_results")
presentation_mod_df = presentation_df.select("Race_year","Driver_name","Race_name","points")
window_mod = presentation_mod_df.groupBy("Race_year","Driver_name")\
.agg(sum("points").alias("points"),countDistinct("Race_name"))\
.orderBy(desc("points"))
winspec = Window.partitionBy("Race_year").orderBy(desc("points"))
window_mod.withColumn("rank",dense_rank().over(winspec)).show(1000)

# COMMAND ----------

Constructors_df = presentation_df.groupBy("Race_year","team").agg(sum("points").alias("points"))
Cons_window_Spec  = Window.partitionBy("Race_year").orderBy(desc("points"))
Constructors_final = Constructors_df.withColumn("Rank", rank().over(Cons_window_Spec))

# COMMAND ----------

display(Constructors_final)

# COMMAND ----------

