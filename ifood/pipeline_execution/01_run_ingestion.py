import sys
import os

# --- TRAVA DE CAMINHO (À prova de falhas) ---
# Descobre a pasta atual (execution) e sobe um nível (..) para a raiz do projeto
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
raiz_projeto = os.path.abspath(os.path.join(diretorio_atual, '..'))

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)
# ---------------------------------------------

# Agora os imports funcionam de forma limpa!
from config.functions import get_runtime_parameters
from src.ingestion_to_landing import download_landing_data

if __name__ == "__main__":
    print("Iniciando Job: Ingestion to Landing...")
    particoes = get_runtime_parameters()
    download_landing_data(particoes)