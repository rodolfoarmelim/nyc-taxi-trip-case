# Databricks notebook source
# # Célula 1 do seu Notebook (Python puro, sem %sh)

import sys
import os

# # 1. (Opcional) Apenas para garantir que a raiz do projeto seja encontrada
# # Se você estiver usando "Databricks Repos / Git Folders", o Databricks já faz isso sozinho.
# # Mas se estiver nas pastas normais do Workspace, mantemos essa trava de segurança:
# folder_path = os.path.abspath('..')
# if folder_path not in sys.path:
#     sys.path.append(folder_path)

# 2. Importa a sua função principal diretamente do arquivo main.py
from src.main import run

# 3. Executa a pipeline
run()

