# Databricks notebook source
from config.functions import get_runtime_parameters
from src.landing_to_bronze import process_landing_to_bronze

# COMMAND ----------

particoes = get_runtime_parameters()
process_landing_to_bronze(particoes)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execução finalizada com sucesso!