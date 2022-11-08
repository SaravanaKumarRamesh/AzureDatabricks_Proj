# Databricks notebook source
from pyspark.sql.types import IntegerType,StringType,StructType,StructField,FloatType,DoubleType,DateType
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.window import Window
raw_folder_path = '/mnt/raw'
processed_folder_path = '/mnt/processed'
Presentation_folder_path = '/mnt/presentation'

# COMMAND ----------

def merge_delta_data(input_df,db_name,table_name,filepath,Merge_condtion,Partition_column):
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
           TargetTbl = DeltaTable.forPath(spark, f"{filepath}/{table_name}")
           TargetTbl.alias("tgt").merge(input_df.alias("src"),Merge_condtion)\
                    .whenMatchedUpdateAll()\
                    .whenNotMatchedInsertAll()\
                    .execute()
    else:
         input_df.write.mode("overwrite").partitionBy(Partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://crick-data-csv@saravanastroagetest.blob.core.windows.net",
  mount_point = "/mnt/crick-data-csv",
  extra_configs = {"fs.azure.account.key.saravanastroagetest.blob.core.windows.net":"vxcw70DJk0sSu/I3XFGIsuVCIOxKdUII75bAhqUeJVha2sGDMWAODfMFXwmLP9PiX+Sh6EI+U4BZpUgiNZcSBA=="})

# COMMAND ----------

dbutils.fs.unmount("/mnt/crick-data-csv")

# COMMAND ----------

dbutils.fs.ls('mnt/')

# COMMAND ----------

dbutils.fs.ls('/mnt/crick-data-csv/IPL_2008_data_csv/[^_info]*.csv')

# COMMAND ----------

 