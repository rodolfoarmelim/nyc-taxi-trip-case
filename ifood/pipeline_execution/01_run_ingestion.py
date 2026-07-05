import sys
import os

# --- TRAVA DE CAMINHO (À prova de falhas) ---
# Descobre a pasta atual (execution) e sobe um nível (..) para a raiz do projeto
folder_path = os.path.abspath('..')
if folder_path not in sys.path:
    sys.path.append(folder_path)
# ---------------------------------------------

# Agora os imports funcionam de forma limpa!
from config.functions import get_runtime_parameters
from src.ingestion_to_landing import download_landing_data

if __name__ == "__main__":
    print("Iniciando Job: Ingestion to Landing...")
    particoes = get_runtime_parameters()
    download_landing_data(particoes)