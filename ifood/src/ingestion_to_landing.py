import requests
from config.settings import BASE_URL, TAXI_COLOR_LIST, LANDING_ZONE_VOLUME_PATH

def download_landing_data(particoes: list):
    """
    Baixa os arquivos Parquet da API do NYC Taxi e salva no Volume do Databricks.
    """
    print("\n=============================================")
    print("--- [STEP 1] INICIANDO: INGESTION TO LANDING ---")
    print("=============================================")

    # Atribui valor as cores de processamento
    cores_para_processar = TAXI_COLOR_LIST
    print(f"[CONFIG] Cores selecionadas para este run: {cores_para_processar}")

    arquivos_baixados = 0

    for color in cores_para_processar:
        for year_month in particoes:
            nome_arquivo = f"{color}_tripdata_{year_month}.parquet"
            
            # Garante que a URL não terá barras duplicadas
            url = f"{BASE_URL}/{nome_arquivo}" if not BASE_URL.endswith('/') else f"{BASE_URL}{nome_arquivo}"
            caminho_volume = f"{LANDING_ZONE_VOLUME_PATH}/{nome_arquivo}" if not LANDING_ZONE_VOLUME_PATH.endswith('/') else f"{LANDING_ZONE_VOLUME_PATH}{nome_arquivo}"
            
            print(f"\n[DOWNLOAD] Baixando arquivo da API: {nome_arquivo}")
            
            try:
                # 1. Requisição HTTP via stream para otimização de memória
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    
                    # 2. Escrita direta no Databricks Volume em chunks
                    print(f"[ESCRITA] Salvando arquivo no Volume: {caminho_volume}")
                    with open(caminho_volume, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            
                print(f"[SUCESSO] {nome_arquivo} baixado e armazenado na Landing Zone.")
                arquivos_baixados += 1
                
            except Exception as e:
                print(f"[ERRO] Falha crítica ao baixar {nome_arquivo}: {str(e)}")
                raise e
                
    print("\n=============================================")
    print(f"--- [STEP 1] SUCESSO: INGESTION TO LANDING FINALIZADO ---")
    print(f"Total de arquivos baixados com sucesso: {arquivos_baixados}")
    print("=============================================\n")