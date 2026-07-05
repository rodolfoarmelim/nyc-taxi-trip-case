# Databricks notebook source
import sys
import os

# COMMAND ----------

# Importando da raiz do projeto nativamente
from config.functions import get_runtime_parameters
from src.ingestion_to_landing import download_landing_data

# COMMAND ----------

# Capturando parâmetros e executando
particoes = get_runtime_parameters()
download_landing_data(particoes)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execução finalizada com sucesso!