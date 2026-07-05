# Databricks notebook source
import sys
import os

# COMMAND ----------

# Importando da raiz do projeto nativamente
from config.functions import get_runtime_parameters
from src.silver_to_gold import process_silver_to_gold

# COMMAND ----------

# Capturando parâmetros e executando
particoes = get_runtime_parameters()
process_silver_to_gold(particoes)

# COMMAND ----------
# MAGIC %md
# MAGIC ### ✅ Transformação Silver -> Gold finalizada com sucesso!