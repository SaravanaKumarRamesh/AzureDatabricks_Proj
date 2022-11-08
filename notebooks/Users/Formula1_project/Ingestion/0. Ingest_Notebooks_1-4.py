# Databricks notebook source
dbutils.notebook.run("1.Data_ingestion_Circuits", 60, {"p_Filedate": "2021-03-21", "p_data_source": "Manual"})

# COMMAND ----------

dbutils.notebook.run("2. Data_ingestion_Races", 60, {"p_Filedate": "2021-03-21", "p_data_source": "Manual"})

# COMMAND ----------

dbutils.notebook.run("3. Data_ingestion_Constructors", 60, {"p_Filedate": "2021-03-21", "p_data_source": "Manual"})

# COMMAND ----------

dbutils.notebook.run("4. Data_Ingestion_drivers", 60, {"p_Filedate": "2021-03-21", "p_data_source": "Manual"})