import sys
import os

folder_path = os.path.abspath('..')
if folder_path not in sys.path:
    sys.path.append(folder_path)

from config.functions import get_runtime_parameters
from src.landing_to_bronze import process_landing_to_bronze

if __name__ == "__main__":
    print("Iniciando Job: Landing to Bronze...")
    particoes = get_runtime_parameters()
    process_landing_to_bronze(particoes)