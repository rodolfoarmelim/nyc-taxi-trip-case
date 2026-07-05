# Databricks notebook source
# MAGIC %md
# MAGIC # Análise exploratória - NYC TLC Data
# MAGIC
# MAGIC O presente notebook agrega abaixo uma análise exploratória de dados das tabelas **nyc_taxi.bronze.tb_bronze_yellow_taxi_trips** e **nyc_taxi.bronze.tb_bronze_green_taxi_trips**, que armazenam os dados brutos de viagem de táxis amarelos e verdes, respectivamente, da cidade de Nova York. O objetivo principal deste documento é embasar as regras de transformação, limpeza e unificação dos dados de ambas as tabelas em sua movimentação da camada bronze para a silver, incluindo:
# MAGIC
# MAGIC - Seleção de Atributos: Inclusão e/ou remoção de colunas não utilizadas;
# MAGIC - Padronização de contratos: Conversão ou renomeação de campos;
# MAGIC - Sanidade dos dados: Tratamento de nulos e zeros, quando aplicável;
# MAGIC - Remoção de outliers para dados numéricos, quando aplicável
# MAGIC
# MAGIC Ao final deste notebook serão consolidados:
# MAGIC - Mapeamento de todas as regras de tratamento dos dados que serão aplicadas
# MAGIC - Schema final da tabela silver **tb_silver_nyc_taxi_trips**, constando os nomes dos campos que a compõem, definição e data type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Análise Táxis Verdes (nyc_taxi.bronze.tb_bronze_green_taxi_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC ### i. Carregando os dados

# COMMAND ----------

## Imports
from pyspark.sql.functions import (
    col, 
    to_utc_timestamp, 
    hour, 
    unix_timestamp, 
    round, 
    datediff, 
    date_format,
    current_timestamp, 
    lit, 
    count, 
    when,
    floor,
) 
import pyspark.sql.functions as F

# COMMAND ----------

df_green = spark.table("nyc_taxi.bronze.tb_bronze_green_taxi_trips")
display(df_green)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ii. Volume de dados total e por partição

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, count, when

# Análise por partição
display(df_green.groupBy(col('year_month_file')).agg( \
    count('*').alias('total_records') \
    ).orderBy(col('year_month_file').asc())
)

# COMMAND ----------

## Análise total
display(df_green.select(count('*').alias('total_records')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### iii. Visualizando os schema

# COMMAND ----------

print("Schema da tabela:")
df_green.printSchema()
print(f"Quantidade de colunas: {len(df_green.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### iv. Summary estatístico da tabela
# MAGIC

# COMMAND ----------

display(df_green.summary())

# COMMAND ----------

## Avaliando se tenho dados nulos de data
display(df_green.filter((col('lpep_pickup_datetime').isNull()) | (col('lpep_dropoff_datetime').isNull())))

## Sem colunas nulas de data

# COMMAND ----------

### Avaliando dados duplicados de forma geral
print(f"Quantidade de linhas geral: {df_green.count()}")
print(f"Quantidade de linhas duplicadas: {df_green.dropDuplicates().count()}")

# Utilizando como chave os dados de vendor id, dropoff e pickup para remover duplicados
print(f"Quantidade de linhas duplicadas por chave composta Vendor + pickup + dropoff: {df_green.dropDuplicates(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID']).count()}")

### Não temos duplicados olhando de forma geral, porém, pela chave vendor e data encontramos valores duplicados.


# COMMAND ----------

from pyspark.sql.window import Window

# 1. Definimos a janela baseada na sua chave composta
# O orderBy garante uma ordenação consistente para o row_number
janela_duplicados = Window.partitionBy(
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance", "DOLocationID", "PULocationID"
)

# 2. Criamos o DataFrame de análise adicionando duas colunas de controle:
# - 'row_num': diz se é a 1ª, 2ª, 3ª aparição daquela combinação
# - 'total_aparicoes': diz quantas vezes aquela combinação exata se repete no dataset todo
df_analise_duplicados = (
    df_green.withColumn(
        "total_aparicoes", F.count("*").over(janela_duplicados)
)
)

# 3. Filtramos para trazer APENAS os casos que se repetem (total_aparicoes > 1)
# E ordenamos pela chave composta para que as linhas duplicadas fiquem juntas na tela
df_visualizacao_final = df_analise_duplicados.filter(
    F.col("total_aparicoes") > 1
).orderBy("VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance", "DOLocationID", "PULocationID")

# 4. Exibe o dataset completo de duplicados de forma fácil no Databricks
display(df_visualizacao_final.count())
display(df_visualizacao_final)

# COMMAND ----------

print(f"Quantidade de registro com trip_distance maior que zero e total_amount maior que zero: {df_visualizacao_final.filter((col('trip_distance') > 0) & (col('total_amount') > 0)).count()}")
print(f"Quantidade de registro com drop duplicates depois do filtro: {df_visualizacao_final.filter((col('trip_distance') > 0) & (col('total_amount') > 0)).dropDuplicates(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID']).count()}")

# COMMAND ----------

df_analise_duplicados_2 = (
    df_visualizacao_final.filter((col('trip_distance') > 0) & (col('total_amount') > 0)).withColumn(
        "total_aparicoes", F.count("*").over(janela_duplicados)
)
)

display(df_analise_duplicados_2.filter(F.col("total_aparicoes") > 1).orderBy("VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance", "total_amount"))

# COMMAND ----------

## contando o número de registros por passenger count
display(df_green.groupBy('passenger_count').count().orderBy(col('passenger_count').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Break 1 - Insights
# MAGIC  Com as análise feitas até o momento somado ao summary, temos conclusões interessantes que nos ajudam e outras que podemos investigar mais a fundo. Listando nos tópicos abaixo:
# MAGIC
# MAGIC  - As colunas **VendorID**, **trip_distance**, colunas de localização e data, além de colunas de pagamento exceto airport_fee e congestion_surchage estão preenchidas com valores por completo (339.630, volumetria completa da tabela)
# MAGIC  - Março de 2023 foi o mês com mais viagens reportadas no período analisado
# MAGIC  - As colunas que possuem valores nulos apresentam sempre a mesma volumetria de dados: 316.734 (delta de ~22k viagens, 6,7% da volumetria total da base)
# MAGIC  - Analisando o valor mínimo de colunas de pagamento e com foco em **total_amount**, visualizamos valores negativos que podem estar distribuídos entre os percentis P0 e P25.
# MAGIC  - A coluna **passenger_count** apresenta dados nulos, zerados e máximo de passageiros até 9 (o que pode ser considerado improvável). Segundo dados do governo de Nova York, o limite de passageiros é de 5 pessoas (https://www.nyc.gov/site/tlc/passengers/passenger-frequently-asked-questions.page)
# MAGIC  - O valor máximo de distância percorrida em **trip_distance** é superior a 267k milhas, além de termos viagens com distância zerada.
# MAGIC  - Temos ~1.000 casos onde, pela chave de **VendorID**, datas de pickup e drop off, **trip_distance** e as localizações de pickup e drop_off temos dados duplicados. Parte destes casos apresentam valores diferentes, porém, boa parte apresenta valores de **total_amount** com um registro positivo e outro negativo. Desta forma, estes duplicados deveram ser eliminados pelos valores negativos. Não temos nenhum caso com valores positivos e diferentes nos duplicados.
# MAGIC  - 
# MAGIC
# MAGIC  Com estes valores e entendendo a composição dos dados, podemos seguir para os próximos passos, que são:
# MAGIC  - Investigar se há correlação dos dados nulos com outras colunas (exemplo: os dados de passenger_count nulos são os mesmos de store_and_fwd_flag)
# MAGIC  - Investigar correlação de dados nulos em **passenger_count**, por exemplo, se correlacionam com as viagens de **total_amount** negativas.
# MAGIC  - Investigar a distribuição da coluna **trip_distance**, buscando remover outliers
# MAGIC  - Investigar a distribuição da coluna **total_amount**, buscando remover os outliers superiores
# MAGIC  - Verificar o conteúdo de datas para identificar viagens com tempos que não fazem sentido
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### v. Analisando colunas nulas + total_amount_nulo

# COMMAND ----------

# Analisando a composição de dados com passenger_count nulo
print(f"Quantidade de linhas com passenger count nulo: {df_green.filter(col('passenger_count').isNull()).count()}")
print(f"Quantidade de linhas nulas com total_amount negativo: {df_green.filter((col('passenger_count').isNull()) & (col('total_amount') < 0)).count()}")

### Somente 8 registros com total_amount negativo e passenger_count nulo

# Quantidade SOMENTE de total_acount_nulo
print(f"Quantidade de linhas com total_amount negativo: {df_green.filter(col('total_amount') < 0).count()}")

# COMMAND ----------

display(df_green.filter(col('passenger_count').isNull()))

# COMMAND ----------

# Distribuição de valores nulos de total amount por partição + de valores nulos
display(df_green.filter(col('total_amount') <= 0).groupBy(col('year_month_file')).agg(count('*').alias('total_records')))
display(df_green.filter(col('passenger_count').isNull()).groupBy(col('year_month_file')).agg(count('*').alias('total_records')))

# COMMAND ----------

# Verificando os valores nulos de passenger_count e os outros distintos
display(df_green.filter(col('passenger_count').isNull()).groupBy('VendorId').count().alias('total_records'))

## Vários casos de Vendor 2 tem valores de passageiro nulo.


# COMMAND ----------

## Verificando contagem de passenger_count distintos e como estão distribuídos os dados de 
display(df_green.groupBy('passenger_count').agg( \
    count('*').alias('count'), \
    count(when(col('total_amount') > 0, 1)).alias('total_positive'), \
    count(when(col('total_amount') <= 0, 1)).alias('total_negative'), \
    ((count(when(col('total_amount') > 0, 1))) / (count('*'))).alias('ratio_positive') \
    )
.orderBy(col('passenger_count')))


##### Sempre mais de 97% dos casos com viagens com total amount positivo.


# COMMAND ----------

# MAGIC %md
# MAGIC ### vi. Analisando dados do ponto de vista de datas

# COMMAND ----------

# 1. Definindo o fuso horário de origem (Nova York)
tz_ny = "America/New_York"

# 2. Criando o DataFrame auxiliar com as colunas obrigatórias + minutos arredondados para baixo
df_datas_silver_base = df_green.select(
    # === COLUNAS OBRIGATÓRIAS DA CAMADA DE CONSUMO ===
    col("VendorID"),
    col("passenger_count"),
    col("total_amount"),
    col("lpep_pickup_datetime"),
    col("lpep_dropoff_datetime"),
    col("trip_distance"),
    col("PULocationID"),
    col("DOLocationID"),
    col("year_month_file"),
    col("RateCodeID"),
    
    # === NOVAS COLUNAS DERIVADAS E AJUSTADAS ===
    # B. Hora do dia em que ocorreu (Baseado no Pickup)
    hour(col("lpep_pickup_datetime")).alias("hora_do_dia_pickup"),
    hour(col("lpep_dropoff_datetime")).alias("hora_do_dia_dropoff"),
    
    # C. Diferença entre dropoff e pickup em HORAS (com 2 casas decimais)
    round(
        (unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))) / 3600, 
        2
    ).alias("duracao_horas"),
    
    # === NOVA COLUNA: Diferença em MINUTOS arredondada para baixo ===
    floor(
        (unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))) / 60
    ).alias("duracao_minutos_floor"),
    
    # D. Diferença entre dropoff e pickup em DIAS
    datediff(col("lpep_dropoff_datetime"), col("lpep_pickup_datetime")).alias("duracao_dias"),
    
    # E. Ano e mês do pickup e do dropoff (Formato YYYY-MM)
    date_format(col("lpep_pickup_datetime"), "yyyy-MM").alias("ano_mes_pickup"),
    date_format(col("lpep_dropoff_datetime"), "yyyy-MM").alias("ano_mes_dropoff")
)

# 3. Exibindo o resultado estruturado no notebook
display(df_datas_silver_base.orderBy(col('duracao_dias').desc()).limit(10))

# COMMAND ----------

## Visualizando quantas viagens com mais de um dia e algumas características destas viagens são necessárias
display(df_datas_silver_base.groupBy('duracao_dias').agg(
    F.count('*').alias('total_records'),
    F.max(F.col('trip_distance')).alias('max_trip_distance'),
    F.min(F.col('trip_distance')).alias('min_trip_distance'),
    F.avg(F.col('trip_distance')).alias('avg_trip_distance'),
    F.max(F.col('total_amount')).alias('max_total_amount'),
    F.min(F.col('total_amount')).alias('min_total_amount'),
    F.avg(F.col('total_amount')).alias('avg_total_amount'),
    F.max(F.col('duracao_horas')).alias('max_duracao_horas'),
    F.avg(F.col('duracao_horas')).alias('avg_duracao_horas')
).orderBy(F.col('duracao_dias').desc()))

### Neste caso, temos 99% das viagens ocorrendo dentro de um dia. Cerca de 2631 viagens com 1 dia de duração.

# COMMAND ----------

## Calculando a quantidade de viagens com distância 0
display(df_datas_silver_base.filter(col('trip_distance') == 0).count())


# COMMAND ----------

### Calculando a quantidade de trip_distance = 0, quantos tem total amount positivo (e seu ratio), quantas tem passenger_count nulo.
display(df_datas_silver_base.filter(col('trip_distance') == 0).groupBy('trip_distance').agg(
    count('*').alias('total_records'),
    count(F.when(col('total_amount') > 0, 1)).alias('total_positive'),
    count(F.when(col('total_amount') <= 0, 1)).alias('total_negative'),
    ((count(F.when(col('total_amount') > 0, 1))) / (count('*'))).alias('ratio_positive'),
    count(F.when(col('passenger_count').isNull(), 1)).alias('total_null_passenger_count'),
    ((count(F.when(col('passenger_count').isNull(), 1))) / (count('*'))).alias('null_passenger_ratio'),
    ).orderBy(col('total_records').desc()))

# 96,5% dos casos com trip_distance = 0 tem total_amount positivo. 4,7% de todos os casos com trip_distance = 0 são casos de passageiros nulos. Casos de trip distance zerados representam entre 4,7% dos casos do dataset. Para termos de análises, vamos desconsiderar estas viagens.

# COMMAND ----------

display(df_datas_silver_base.filter((col('trip_distance') == 0) & (col('passenger_count').isNull())))

### Aqui temos dados interessantes: temos casos de trip distance 0 com tempos de viagem muito baixos. Ou que até mesmo não mudaram de zona. Isso é interessante investigar.


# COMMAND ----------

display(df_datas_silver_base.filter((col('trip_distance') == 0)).orderBy(col('duracao_minutos_floor').desc()))

### Dados com duração 0

display(df_datas_silver_base.filter(col('duracao_minutos_floor') <= 0))

# COMMAND ----------

### Verificando se temos datas com mês-ano de pickup diferente da partição do arquivo
display(df_datas_silver_base.filter(col('ano_mes_pickup') != col('year_month_file')).count())
display(df_datas_silver_base.filter(col('ano_mes_pickup') != col('year_month_file')).orderBy(col('ano_mes_pickup').asc()))

# COMMAND ----------

# 1. Base de cálculo aplicando F.abs() para unificar os desvios
df_analise_meses = (df_datas_silver_base
    .withColumn("data_ref_arquivo", F.to_date(F.col("year_month_file"), "yyyy-MM"))
    .withColumn("data_ref_pickup", F.to_date(F.col("ano_mes_pickup"), "yyyy-MM"))
    .withColumn("data_ref_dropoff", F.to_date(F.col("ano_mes_dropoff"), "yyyy-MM"))
    .withColumn(
        "desvio_absoluto_meses_pickup", 
        F.abs(F.round(F.months_between(F.col("data_ref_pickup"), F.col("data_ref_arquivo")), 0)).cast("int")
    )
    .withColumn(
        "desvio_absoluto_meses_dropoff", 
        F.abs(F.round(F.months_between(F.col("data_ref_dropoff"), F.col("data_ref_arquivo")), 0)).cast("int")
    )
)

# 2. Agrupamento isolado de PICKUP
df_resumo_pickup = (df_analise_meses
    .filter(F.col("desvio_absoluto_meses_pickup") > 0)
    .groupBy("desvio_absoluto_meses_pickup")
    .agg(F.count("*").alias("linhas_afetadas_pickup"))
)

# 3. Agrupamento isolado de DROPOFF
df_resumo_dropoff = (df_analise_meses
    .filter(F.col("desvio_absoluto_meses_dropoff") > 0)
    .groupBy("desvio_absoluto_meses_dropoff")
    .agg(F.count("*").alias("linhas_afetadas_dropoff"))
)

# 4. Unificação das duas tabelas via FULL JOIN pela chave do desvio
df_consolidado = (df_resumo_pickup
    .join(df_resumo_dropoff, df_resumo_pickup.desvio_absoluto_meses_pickup == df_resumo_dropoff.desvio_absoluto_meses_dropoff, how="full")
    # O coalesce garante que pegamos o número do desvio independente de qual tabela ele veio no Full Join
    .withColumn("Desvio Absoluto (Meses)", F.coalesce(F.col("desvio_absoluto_meses_pickup"), F.col("desvio_absoluto_meses_dropoff")))
    # Substitui nulos por 0 caso um desvio específico só tenha ocorrido em um dos lados
    .withColumn("Total Linhas (Pickup)", F.coalesce(F.col("linhas_afetadas_pickup"), F.lit(0)))
    .withColumn("Total Linhas (Dropoff)", F.coalesce(F.col("linhas_afetadas_dropoff"), F.lit(0)))
    # Seleciona e ordena o relatório final
    .select("Desvio Absoluto (Meses)", "Total Linhas (Pickup)", "Total Linhas (Dropoff)")
    .orderBy("Desvio Absoluto (Meses)")
)

# 5. Exibe a tabela comparativa final
display(df_consolidado)

##### Temos grande parte dos casos armazenados onde a diferença entre os meses é nula, com alguns casos de 1. Vamos considerar o pickup como referência e remover casos onde a diferença de meses entre o arquivo e a data de pickup é superior a 0, ou seja, itens que não são do mesmo mês

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primeiras regras de tratamento:
# MAGIC
# MAGIC - Remover viagens com total_amount <= 0
# MAGIC - Remover viagens onde a duração em minutos é <= 0 (corridas muito rápidas)
# MAGIC - Remover viagens onde a duração da corrida é >= 1 dias
# MAGIC - Remover viagens Trip distance <= 0
# MAGIC - Remover viagens na qual a diferença absoluta entre os meses da referência do arquivo e data de pickup > 0
# MAGIC - Remover viagens em que o a contagem de passageiro é superior a 6 (brecha pelo volume encontrado)
# MAGIC
# MAGIC ##### Porque não remover casos de passenger count nulo ou 0?
# MAGIC Porque existem muitas viagens apresentam dados de total_amount positivos e válidos e devem ser considerados para estes casos. Assim, a premissa de utilizar total_amount como fonte da verdade torna necessário a criação de uma coluna de flag para identificar se tivemos passageiros reportados ou não

# COMMAND ----------

### Simulando o tratamento de dados e preparando a base para a análise de outliers
# 1. Aplicando as regras de tratamento (Filtros Lógicos + Filtro Temporal de Desvio de Meses)
df_clean_logica = (df_datas_silver_base
    # Primeiro, criamos uma coluna temporária com o cálculo do desvio absoluto de meses do dropoff
    .withColumn("data_ref_arquivo", F.to_date(F.col("year_month_file"), "yyyy-MM"))
    .withColumn("data_ref_pickup", F.to_date(F.col("ano_mes_pickup"), "yyyy-MM"))
    .withColumn(
        "desvio_absoluto_meses_pickup", 
        F.abs(F.round(F.months_between(F.col("data_ref_pickup"), F.col("data_ref_arquivo")), 0)).cast("int")
    )
    # Agora aplicamos todos os filtros em cadeia, incluindo o limite de desvio de meses
    .filter(
        (F.col("total_amount") > 0) &                            # Remove total_amount <= 0
        (F.col("duracao_minutos_floor") > 0) &                   # Remove duração em minutos <= 0
        (F.col("duracao_dias") < 1) &                            # Remove duração >= 1 dias
        (F.col("trip_distance") > 0) &                           # Remove trip_distance <= 0
        (F.col("desvio_absoluto_meses_pickup") = 0)  &         # Remove desvios de meses superiores a 1
        ((F.col("passenger_count") <= 6) | (F.col('passenger_count').isNull()))                          # Remove passageiros > 6          
    )
    # Removemos as colunas de data temporárias para não poluir o DataFrame final
    .drop("data_ref_arquivo", "data_ref_dropoff", "desvio_absoluto_meses_pickup")
)

print(df_clean_logica.count())
# 2. Criando a coluna de Flag para Passenger Count
df_silver_pre_outliers = df_clean_logica.withColumn(
    "flag_sem_passageiro",
    F.when((F.col("passenger_count").isNull()) | (F.col("passenger_count") <= 0), 1).otherwise(0)
)

# 3. Verificando o volume resultante após a limpeza combinada
print(f"Volume original: {df_datas_silver_base.count()} linhas")
print(f"Volume pós-filtros lógicos e temporais: {df_silver_pre_outliers.count()} linhas")

display(df_silver_pre_outliers.limit(10))

# COMMAND ----------

#### Checando dados duplicados
print(f"Quantidade de linhas geral: {df_silver_pre_outliers.count()}")
print(f"Quantidade de linhas duplicadas: {df_silver_pre_outliers.dropDuplicates().count()}")

# Utilizando como chave os dados de vendor id, dropoff e pickup para remover duplicados
print(f"Quantidade de linhas duplicadas por chave composta Vendor + pickup + dropoff: {df_silver_pre_outliers.dropDuplicates(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID']).count()}")

# COMMAND ----------

display(df_silver_pre_outliers.summary())

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 1. Gerar uma lista de percentis de 0.0 a 1.0 (passo de 0.01)
percentis = np.linspace(0, 1, 101).tolist()

# 2. Calcular os valores dos percentis para as 3 variáveis no PySpark
# Usamos uma aproximação com alta precisão (9999) para mapear a cauda corretamente
df_percentis = df_silver_pre_outliers.select(
    F.percentile_approx("total_amount", percentis, 9999).alias("total_amount"),
    F.percentile_approx("trip_distance", percentis, 9999).alias("trip_distance"),
    F.percentile_approx("duracao_minutos_floor", percentis, 9999).alias("duracao_minutos_floor")
).collect()[0]

# 3. Converter os resultados coletados para um DataFrame do Pandas para plotagem
df_plot_percentis = pd.DataFrame({
    "Percentil": np.linspace(0, 100, 101),
    "Total Amount": df_percentis["total_amount"],
    "Trip Distance": df_percentis["trip_distance"],
    "Duração (Minutos)": df_percentis["duracao_minutos_floor"]
})

# 4. Plotagem dos 3 gráficos de linha lado a lado
fig, axes = plt.subplots(1, 3, figsize=(20, 6))
fig.suptitle("Análise de Cauda Longa: Distribuição por Percentil (0% a 100%)", fontsize=16, fontweight='bold')

# Gráfico 1: Total Amount
axes[0].plot(df_plot_percentis["Percentil"], df_plot_percentis["Total Amount"], color="gold", linewidth=2.5)
axes[0].set_title("Percentis - Total Amount ($)")
axes[0].set_xlabel("Percentil")
axes[0].set_ylabel("Valor em Dólar")
axes[0].grid(True, linestyle="--", alpha=0.6)

# Gráfico 2: Trip Distance
axes[1].plot(df_plot_percentis["Percentil"], df_plot_percentis["Trip Distance"], color="skyblue", linewidth=2.5)
axes[1].set_title("Percentis - Trip Distance (Milhas)")
axes[1].set_xlabel("Percentil")
axes[1].set_ylabel("Distância em Milhas")
axes[1].grid(True, linestyle="--", alpha=0.6)

# Gráfico 3: Duração em Minutos
axes[2].plot(df_plot_percentis["Percentil"], df_plot_percentis["Duração (Minutos)"], color="salmon", linewidth=2.5)
axes[2].set_title("Percentis - Duração (Minutos)")
axes[2].set_xlabel("Percentil")
axes[2].set_ylabel("Tempo em Minutos")
axes[2].grid(True, linestyle="--", alpha=0.6)

plt.tight_layout()
plt.show()

# COMMAND ----------

import seaborn as sns

# 1. Obter uma amostra segura para o gráfico de dispersão
df_scatter_pandas = df_silver_pre_outliers.select(
    "total_amount", "trip_distance", "duracao_minutos_floor"
).sample(False, 0.1, seed=42).limit(100000).toPandas()

# 2. Configurar a matriz de gráficos de dispersão (1 linha, 3 colunas)
fig, axes = plt.subplots(1, 3, figsize=(22, 6))
fig.suptitle("Análise de Dispersão: Identificação de Outliers Cruzados", fontsize=16, fontweight='bold')

# Correlação 1: Distância vs Preço
sns.scatterplot(data=df_scatter_pandas, x="trip_distance", y="total_amount", ax=axes[0], alpha=0.5, color="purple")
axes[0].set_title("Distância (mi) vs Total Amount ($)")
axes[0].set_xlabel("Trip Distance (Milhas)")
axes[0].set_ylabel("Total Amount ($)")

# Correlação 2: Duração vs Preço
sns.scatterplot(data=df_scatter_pandas, x="duracao_minutos_floor", y="total_amount", ax=axes[1], alpha=0.5, color="teal")
axes[1].set_title("Duração (min) vs Total Amount ($)")
axes[1].set_xlabel("Duração (Minutos)")
axes[1].set_ylabel("Total Amount ($)")

# Correlação 3: Duração vs Distância
sns.scatterplot(data=df_scatter_pandas, x="duracao_minutos_floor", y="trip_distance", ax=axes[2], alpha=0.5, color="coral")
axes[2].set_title("Duração (min) vs Distância (mi)")
axes[2].set_xlabel("Duração (Minutos)")
axes[2].set_ylabel("Trip Distance (Milhas)")

plt.tight_layout()
plt.show()

# COMMAND ----------

import pyspark.sql.functions as F

# 1. Definição da lista de percentis (exatamente igual ao seu código anterior)
lista_percentis = [0.5, 0.6, 0.7, 0.8] + [x / 100 for x in range(90, 101)]

# 2. Calcula as listas de percentis para cada coluna em PySpark
df_array = df_silver_pre_outliers.select(
    F.percentile_approx("total_amount", lista_percentis, 9999).alias("total_amount_arr"),
    F.percentile_approx("trip_distance", lista_percentis, 9999).alias("trip_distance_arr"),
    F.percentile_approx("duracao_minutos_floor", lista_percentis, 9999).alias("duracao_minutos_arr")
)

# 3. Explodimos cada array individualmente trazendo o índice 'pos' já tratado como INT
df_amt = df_array.select(F.posexplode("total_amount_arr").alias("pos", "total_amount")).withColumn("pos", F.col("pos").cast("int"))
df_dst = df_array.select(F.posexplode("trip_distance_arr").alias("pos_d", "trip_distance")).withColumn("pos_d", F.col("pos_d").cast("int"))
df_dur = df_array.select(F.posexplode("duracao_minutos_arr").alias("pos_t", "duracao_minutos")).withColumn("pos_t", F.col("pos_t").cast("int"))

# 4. Unimos as tabelas de percentis utilizando a posição inteira (Join seguro)
df_percentis_spark = (df_amt
    .join(df_dst, df_amt.pos == df_dst.pos_d)
    .join(df_dur, df_amt.pos == df_dur.pos_t)
    .select(
        df_amt.pos,
        F.round("total_amount", 2).alias("Total Amount ($)"),
        F.round("trip_distance", 2).alias("Trip Distance (mi)"),
        F.col("duracao_minutos").cast("int").alias("Duração (min)")
    )
)

# 5. Criamos o mapeamento dos nomes de texto (P50, P60...) baseados no índice 'pos' numérico
mapeamento_nomes = [F.when(F.col("pos") == idx, f"P{int(p*100)}") for idx, p in enumerate(lista_percentis)]

df_tabela_final = df_percentis_spark.withColumn(
    "Percentil", 
    F.coalesce(*mapeamento_nomes)
).select("Percentil", "Total Amount ($)", "Trip Distance (mi)", "Duração (min)")

# 6. Exibe o resultado de forma pura e segura
display(df_tabela_final)

# COMMAND ----------

import pyspark.sql.functions as F

# 1. Calculando os limites exatos do P99 para as 3 variáveis
# Usamos um erro relativo baixo (0.001) para garantir alta precisão no cálculo
p99_limites = df_silver_pre_outliers.approxQuantile(
    ["trip_distance", "total_amount", "duracao_minutos_floor"], 
    [0.99], 
    0.001
)

p99_trip = p99_limites[0][0]
p99_amount = p99_limites[1][0]
p99_duracao = p99_limites[2][0]

# 2. Contagens individuais e combinadas
vol_total = df_silver_pre_outliers.count()
vol_trip = df_silver_pre_outliers.filter(F.col("trip_distance") > p99_trip).count()
vol_amount = df_silver_pre_outliers.filter(F.col("total_amount") > p99_amount).count()
vol_duracao = df_silver_pre_outliers.filter(F.col("duracao_minutos_floor") > p99_duracao).count()

vol_combinado = df_silver_pre_outliers.filter(
    (F.col("trip_distance") > p99_trip) |
    (F.col("total_amount") > p99_amount) |
    (F.col("duracao_minutos_floor") > p99_duracao)
).count()

# 3. Prints sequenciais formatados
print("=== ANÁLISE DE VOLUMETRIA: OUTLIERS P99 ===")
print(f"Volume total de registros: {vol_total:,}".replace(",", "."))
print(f"Volume com trip_distance > P99 ({p99_trip:.2f}): {vol_trip:,}".replace(",", "."))
print(f"Volume com total_amount > P99 ({p99_amount:.2f}): {vol_amount:,}".replace(",", "."))
print(f"Volume com duracao_minutos > P99 ({p99_duracao:.0f}): {vol_duracao:,}".replace(",", "."))
print(f"Volume combinando as 3 regras: {vol_combinado:,}".replace(",", "."))

# COMMAND ----------

import pyspark.sql.functions as F

# 1. Criando o dataset com as variáveis derivadas usando a coluna existente
df_metricas_derivadas = df_silver_pre_outliers.withColumn(
    "tarifa_por_milha", 
    F.round(F.col("total_amount") / F.col("trip_distance"), 2)
).withColumn(
    "velocidade_mph", 
    # Trava de segurança para evitar divisão por zero por conta de arredondamentos
    F.when(F.col("duracao_horas") > 0, F.round(F.col("trip_distance") / F.col("duracao_horas"), 2))
     .otherwise(0)
)

# 2. Definição da lista de percentis
lista_percentis = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8] + [x / 100 for x in range(90, 101)]

# 3. Calcula as listas de percentis para as novas colunas
df_array_derivadas = df_metricas_derivadas.select(
    F.percentile_approx("tarifa_por_milha", lista_percentis, 9999).alias("tarifa_arr"),
    F.percentile_approx("velocidade_mph", lista_percentis, 9999).alias("velocidade_arr")
)

# 4. Explodimos cada array trazendo o índice 'pos' já tratado como INT
df_tar = df_array_derivadas.select(F.posexplode("tarifa_arr").alias("pos", "tarifa")).withColumn("pos", F.col("pos").cast("int"))
df_vel = df_array_derivadas.select(F.posexplode("velocidade_arr").alias("pos_v", "velocidade")).withColumn("pos_v", F.col("pos_v").cast("int"))

# 5. Unimos as tabelas utilizando a posição inteira
df_percentis_derivadas = (df_tar
    .join(df_vel, df_tar.pos == df_vel.pos_v)
    .select(
        df_tar.pos,
        F.round("tarifa", 2).alias("Tarifa por Milha ($/mi)"),
        F.round("velocidade", 2).alias("Velocidade (mph)")
    )
)

# 6. Criamos o mapeamento dos nomes de texto e finalizamos a tabela
mapeamento_nomes = [F.when(F.col("pos") == idx, f"P{int(p*100)}") for idx, p in enumerate(lista_percentis)]

df_tabela_taxas = df_percentis_derivadas.withColumn(
    "Percentil", 
    F.coalesce(*mapeamento_nomes)
).select("Percentil", "Tarifa por Milha ($/mi)", "Velocidade (mph)")

# 7. Exibe a tabela analítica final
display(df_tabela_taxas)

# COMMAND ----------

## display(df_metricas_derivadas.orderBy(col('trip_distance').desc()))
## display(df_metricas_derivadas.filter(col('tarifa_por_milha') == 0).count())
display(df_metricas_derivadas.orderBy(col('tarifa_por_milha').asc()))
##print(f"{df_metricas_derivadas.filter(col('velocidade_mph') > 100).count()}")

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

# 1. Gerar uma lista de percentis de 0.0 a 1.0 (passo de 0.01)
percentis = np.linspace(0, 1, 101).tolist()

# 2. Calcular os valores exatos dos percentis no PySpark
# Usamos alta precisão (9999) para mapear a cauda perfeitamente
df_percentis_derivadas = df_metricas_derivadas.select(
    F.percentile_approx("tarifa_por_milha", percentis, 9999).alias("tarifa_por_milha"),
    F.percentile_approx("velocidade_mph", percentis, 9999).alias("velocidade_mph")
).collect()[0]

# 3. Converter os resultados coletados para um DataFrame do Pandas para plotagem
df_plot_derivadas = pd.DataFrame({
    "Percentil": np.linspace(0, 100, 101),
    "Tarifa por Milha ($/mi)": df_percentis_derivadas["tarifa_por_milha"],
    "Velocidade (mph)": df_percentis_derivadas["velocidade_mph"]
})

# 4. Plotagem dos gráficos de linha (Lado a Lado)
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle("Análise de Cauda Longa: Variáveis Derivadas (0% a 100%)", fontsize=16, fontweight='bold')

# Gráfico 1: Tarifa por Milha
axes[0].plot(df_plot_derivadas["Percentil"], df_plot_derivadas["Tarifa por Milha ($/mi)"], color="mediumpurple", linewidth=2.5)
axes[0].set_title("Distribuição: Tarifa por Milha ($/mi)")
axes[0].set_xlabel("Percentil")
axes[0].set_ylabel("Valor ($/mi)")
axes[0].grid(True, linestyle="--", alpha=0.6)

# Gráfico 2: Velocidade
axes[1].plot(df_plot_derivadas["Percentil"], df_plot_derivadas["Velocidade (mph)"], color="lightgreen", linewidth=2.5)
axes[1].set_title("Distribuição: Velocidade (mph)")
axes[1].set_xlabel("Percentil")
axes[1].set_ylabel("Velocidade (mph)")
axes[1].grid(True, linestyle="--", alpha=0.6)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insights mais definição de segundo tratamento
# MAGIC
# MAGIC
# MAGIC - Avaliando as variáveis **trip_distance**, **total_amount** e **duracao_minutos_floor**, vemos uma distribuição de calda longa, onde até P99 os valores fazem sentido dentro de uma viagem e valores entre P99 e P100 apresentam outliers muito altos.
# MAGIC - As comparação entre as medianas das três variáveis variam entre **3 e 4 vezes** o valor de P99. Porém, existem valores dentro destes casos que podem ser reais. Exemplos são viagens de distância maiores que 15 milhas, por exemplo, que podem tanto custar mais de USD 80 e também durar próximo de 1 hora (todos valores aproximados de P99). Assim, calculamos valores de **tarifa_por_milha** e **velocidade_mph**.
# MAGIC - Para estes dados, a distribuição mostra que P1, as medianas (P50) e P99 destes valores são, aproximadamente:
# MAGIC
# MAGIC -- **tarifa_por_milha** ($/mi): 4,01 ; 9,04; 29,02
# MAGIC -- **velocidade_mph**: 3,76; 10,56; 31,6
# MAGIC
# MAGIC - Os dados mostrados acima são completamente factíveis em viagens normais e não serão desconsiderados de forma puramente estatísica. Com dados externos do governo de Nova York (https://www.dot.ny.gov/divisions/operating/oom/transportation-systems/repository/TSMI-17-05.pdf, https://dmv.ny.gov/new-york-state-drivers-manual-and-practice-tests/chapter-10-special-driving-conditions) em algumas highways pode ser de 65 mph, enquanto dentro dos 5 distritos é comumente 25 mph. Desta forma, para considerar viagens válidas, **serão removidos valores com velocidade muito baixa (abaixo de P1) e estabelecer um limite superior de 80 mph**
# MAGIC - Em prol da segurança de dados de tarifas por milha e para manter maior parte das viagens possíveis, vamos considerar o fator 2 para P1 e P99, ou seja, valores abaixo da metade de P1 e acima do dobro de P99 **serão removidos.**

# COMMAND ----------

import pyspark.sql.functions as F

# ==============================================================================
# PARTE 0: SIMULAÇÃO DA COLUNA DE REFERÊNCIA DO ARQUIVO
# (Caso o df_green ainda não tenha a coluna year_month_file, 
# ajustamos aqui para o script rodar. Exemplo: arquivo de Jan/2023)
# ==============================================================================
if "year_month_file" not in df_green.columns:
    df_green = df_green.withColumn("year_month_file", F.lit("2023-01"))

volume_original = df_green.count()

# ==============================================================================
# PARTE 1: CRIAÇÃO DE COLUNAS AUXILIARES E PRIMEIRO TRATAMENTO (FILTROS LÓGICOS)
# ==============================================================================

df_aux = (df_green
    # 1.1. Cálculos de Tempo e Duração
    .withColumn("duracao_segundos", F.unix_timestamp("lpep_dropoff_datetime") - F.unix_timestamp("lpep_pickup_datetime"))
    .withColumn("duracao_minutos_floor", F.floor(F.col("duracao_segundos") / 60))
    .withColumn("duracao_horas", F.col("duracao_segundos") / 3600)
    .withColumn("duracao_dias", F.col("duracao_segundos") / 86400)
    
    # 1.2. Cálculos de Data para o filtro de vazamento
    .withColumn("ano_mes_pickup", F.date_format("lpep_pickup_datetime", "yyyy-MM"))
    .withColumn("data_ref_arquivo", F.to_date(F.col("year_month_file"), "yyyy-MM"))
    .withColumn("data_ref_pickup", F.to_date(F.col("ano_mes_pickup"), "yyyy-MM"))
    .withColumn(
        "desvio_absoluto_meses", 
        F.abs(F.round(F.months_between(F.col("data_ref_pickup"), F.col("data_ref_arquivo")), 0)).cast("int")
    )
)

# 1.3. Aplicação dos Filtros Lógicos (Primeiro Tratamento)
df_clean_logica = (df_aux.filter(
        (F.col("total_amount") > 0) &                            # Valor financeiro válido
        (F.col("duracao_minutos_floor") > 0) &                   # Remove duração <= 0
        (F.col("duracao_dias") < 1) &                            # Remove viagens de 1 dia ou mais
        (F.col("trip_distance") > 0) &                           # Distância válida
        (F.col("desvio_absoluto_meses") = 0) &                  # Tolerância de 0 mês no vazamento de arquivo
        ((F.col('passenger_count') <= 6) | (F.col('passenger_count').isNull())) # Passageiros <= 6 ou nulo                    
    )
    # 1.4. Flag de Passageiros
    .withColumn(
        "flag_sem_passageiro",
        F.when((F.col("passenger_count").isNull()) | (F.col("passenger_count") <= 0), 1).otherwise(0)
    )
    # Limpeza de colunas temporárias de data
    .drop("duracao_segundos", "data_ref_arquivo", "data_ref_dropoff", "desvio_absoluto_meses")
)

volume_pos_logica = df_clean_logica.count()

# ==============================================================================
# PARTE 2: VARIÁVEIS DERIVADAS E CÁLCULO DE PERCENTIS
# ==============================================================================

# 2.1. Criação das variáveis de Velocidade e Tarifa
df_metricas = (df_clean_logica
    .withColumn("tarifa_por_milha", F.col("total_amount") / F.col("trip_distance"))
    .withColumn(
        "velocidade_mph", 
        F.when(F.col("duracao_horas") > 0, F.col("trip_distance") / F.col("duracao_horas")).otherwise(0)
    )
)

# 2.2. Extração dos limites estatísticos (Percentis 1 e 99)
limites_estatisticos = df_metricas.approxQuantile(
    ["tarifa_por_milha", "velocidade_mph"], 
    [0.01, 0.99], 
    0.001
)

p1_tarifa, p99_tarifa = limites_estatisticos[0][0], limites_estatisticos[0][1]
p1_vel = limites_estatisticos[1][0]

# ==============================================================================
# PARTE 3: SEGUNDO TRATAMENTO (REGRAS DE NEGÓCIO E LIMITES FINAIS)
# ==============================================================================

# Definição dos limites de corte baseados nas regras de negócio estipuladas
limite_inf_tarifa = p1_tarifa / 2
limite_sup_tarifa = p99_tarifa * 2
limite_inf_vel = p1_vel
limite_sup_vel = 80.0  # Teto de negócio fixo (Leis de Trânsito NY)

# 3.1. Aplicação do filtro final
df_silver_final = df_metricas.filter(
    (F.col("tarifa_por_milha") >= limite_inf_tarifa) &
    (F.col("tarifa_por_milha") <= limite_sup_tarifa) &
    (F.col("velocidade_mph") >= limite_inf_vel) &
    (F.col("velocidade_mph") <= limite_sup_vel)
)

volume_final = df_silver_final.count()

# ==============================================================================
# PARTE 4: RELATÓRIO DE PROCESSAMENTO
# ==============================================================================

print("=== RELATÓRIO DE PROCESSAMENTO: CAMADA SILVER ===")
print(f"1. Volume Original (df_green): {volume_original:,}".replace(",", "."))
print(f"2. Volume após Filtros Lógicos: {volume_pos_logica:,} (Removidas: {volume_original - volume_pos_logica:,})".replace(",", "."))
print("-" * 55)
print("=== PARÂMETROS ESTATÍSTICOS EXTRAÍDOS ===")
print(f"Tarifa por Milha ($/mi) -> Limite Inferior (P1/2): {limite_inf_tarifa:.2f} | Limite Superior (P99*2): {limite_sup_tarifa:.2f}")
print(f"Velocidade (mph)        -> Limite Inferior (P1): {limite_inf_vel:.2f} | Limite Superior (Teto NY): {limite_sup_vel:.2f}")
print("-" * 55)
print(f"3. Volume Final Silver: {volume_final:,} (Removidas na Etapa 2: {volume_pos_logica - volume_final:,})".replace(",", "."))
print(f"-> Retenção Total: {(volume_final / volume_original) * 100:.2f}% dos dados originais preservados.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Tratamento final de dados

# COMMAND ----------

# MAGIC %md
# MAGIC Para garantir a qualidade e a integridade da base de dados, o processo de higienização foi dividido em duas etapas principais: a aplicação de filtros lógicos para inconsistências sistêmicas e o tratamento focado em outliers utilizando variáveis derivadas e regras de negócio.
# MAGIC
# MAGIC ### 1. Filtros Lógicos Iniciais (Primeiro Tratamento)
# MAGIC Esta etapa visa expurgar registros fisicamente impossíveis ou corrompidos por falhas de sistema, estabelecendo uma base sólida para as análises subsequentes:
# MAGIC
# MAGIC - **Valores Financeiros Inválidos:** Remoção de viagens com total_amount <= 0.
# MAGIC
# MAGIC - **Duração Irreal (Limites Inferiores):** Remoção de viagens onde a duração da corrida é <= 0 minutos (corridas rápidas demais ou falhas de taxímetro).
# MAGIC
# MAGIC - **Duração Irreal (Limites Superiores):** Remoção de viagens onde a duração da corrida é >= 1 dia.
# MAGIC
# MAGIC - **Distância Nula ou Negativa:** Remoção de viagens com trip_distance <= 0.
# MAGIC
# MAGIC - **Desvios Temporais (Vazamento de Partição):** Remoção de registros onde a diferença absoluta entre o mês/ano de referência do arquivo e a data real de pickup seja > 0
# MAGIC
# MAGIC **Tratamento da Variável passenger_count**
# MAGIC Existem inúmeras viagens na base que não reportaram a quantidade de passageiros (passenger_count nulo ou igual a 0), mas que apresentam dados financeiros (total_amount) positivos e perfeitamente válidos. Como a premissa do projeto é utilizar o total_amount como fonte da verdade para o faturamento, essas viagens não serão removidas. Em vez disso, será criada uma coluna de flag (sinalizador) para identificar se a corrida teve passageiros reportados ou não, preservando a integridade financeira do dataset.
# MAGIC
# MAGIC ### 2. Tratamento de Outliers e Regras de Negócio (Segundo Tratamento)
# MAGIC A avaliação das variáveis absolutas (trip_distance, total_amount e duracao_minutos_floor) revelou uma forte distribuição de cauda longa. Notou-se que os valores são consistentes até o percentil 99 (P99), sofrendo uma explosão anômala apenas entre o P99 e o P100.
# MAGIC
# MAGIC Cortar os dados unicamente com base em tetos estatísticos absolutos resultaria na perda de dados reais. Viagens longas autênticas (como trajetos para aeroportos com mais de 15 milhas, durações próximas a 1 hora ou custos acima de USD 80) estariam em risco. Para contornar isso, a abordagem adotada foi a criação e análise de variáveis derivadas: Tarifa por Milha (tarifa_por_milha) e Velocidade (velocidade_mph).
# MAGIC
# MAGIC A distribuição estatística destas variáveis derivadas (P1, P50/Mediana e P99) demonstrou os seguintes patamares:
# MAGIC
# MAGIC - **Tarifa por Milha ($/mi)**: P1 = 4,01 | P50 = 9,04 | P99 = 29,02
# MAGIC
# MAGIC - **Velocidade (mph):** P1 = 3,76 | P50 = 10,56 | P99 = 31,6
# MAGIC
# MAGIC
# MAGIC Como os valores até o P99 são completamente factíveis para a realidade do trânsito urbano, o expurgo puramente estatístico (como a regra padrão do IQR) foi descartado em favor de regras de negócio baseadas na legislação de Nova York e em margens de segurança.
# MAGIC
# MAGIC - **Definição dos Limites Finais de Expurgo:**
# MAGIC
# MAGIC *Filtro de Velocidade (mph):*
# MAGIC
# MAGIC *Justificativa:* Dados oficiais do governo e do Departamento de Veículos Automotores de Nova York (NYS DMV / NYS DOT) indicam que o limite urbano comum é de 25 mph, enquanto as highways intermunicipais ou expressas podem atingir o teto de 65 mph.
# MAGIC
# MAGIC *Regra Aplicada:* Serão removidos registros com velocidade média excessivamente baixa (abaixo do P1: 3,16 mph) e aplicado um limite de corte superior rígido para anomalias de GPS de 80 mph.
# MAGIC
# MAGIC **Filtro de Tarifa por Milha ($/mi):**
# MAGIC
# MAGIC *Justificativa:* Para garantir a segurança dos dados financeiros e reter a maior parte das viagens possíveis, incluindo precificações dinâmicas e congestionamentos atípicos.
# MAGIC
# MAGIC *Regra Aplicada:* Será utilizado um fator de tolerância 2x sobre as pontas da curva. Serão removidos os valores que ficarem abaixo da metade do P1 (< 2,00 $/mi) e acima do dobro do P99 (> 58,04 $/mi).

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Schema final da tabela silver (parte de yellow trips)
# MAGIC
# MAGIC Com isso, já temos as colunas que podemos utilizar como referência para criar o schema da tabela da camada silver.
# MAGIC
# MAGIC Como boa parte das colunas também é apresentada dentro da tabela green, vamos criar colunas que possam ser calculadas pelos 2 datasets e deixar registrado aqui. O schema será:
# MAGIC
# MAGIC - **vendor_id** (Integer / String): ID do fornecedor do sistema de táxi.
# MAGIC
# MAGIC - **passenger_count** (Integer): Número de passageiros registrados na viagem.
# MAGIC
# MAGIC - **total_amount** (Double / Float): Valor total cobrado pela corrida (nossa fonte da verdade).
# MAGIC
# MAGIC - **pickup_datetime** (Timestamp): Data e hora exata do início da viagem (renomeado de lpep_pickup_datetime).
# MAGIC
# MAGIC - **dropoff_datetime** (Timestamp): Data e hora exata do fim da viagem (renomeado de lpep_dropoff_datetime).
# MAGIC
# MAGIC - **taxi_color** (String): Cor do táxi ('yellow' fixado para este dataset).
# MAGIC
# MAGIC - **pickup_time_hour** (Integer): Apenas a hora (0-23) extraída do pickup.
# MAGIC
# MAGIC - **pickup_time_year_month** (Date): Componente YYYY-MM da data de pickup.
# MAGIC
# MAGIC - **dropoff_time_hour** (Integer): Apenas a hora (0-23) extraída do dropoff.
# MAGIC
# MAGIC - **dropoff_time_pickup_month** (Date): Componente YYYY-MM da data de dropoff
# MAGIC
# MAGIC - **is_passenger_count_recorded** (Boolean): True se o número de passageiros for reportado (maior que zero) e False caso seja nulo ou zero.
# MAGIC
# MAGIC - **dt_ingestion** (Timestamp): Data e hora do momento do processamento (carimbo de auditoria).
# MAGIC
# MAGIC - **year_month_file** (String): Referência de processamento da partição do arquivo original (formato YYYY-MM).
# MAGIC

# COMMAND ----------

