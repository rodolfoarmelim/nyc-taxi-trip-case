# Databricks notebook source
import sys
import os

# COMMAND ----------

# Importando da raiz do projeto nativamente
from config.functions import get_runtime_parameters
from src.bronze_to_silver import process_bronze_to_silver

# COMMAND ----------

# Capturando parâmetros e executando
particoes = get_runtime_parameters()
process_bronze_to_silver(particoes)

# COMMAND ----------
# MAGIC %md
# MAGIC ### ✅ Transformação Bronze -> Silver finalizada com sucesso!