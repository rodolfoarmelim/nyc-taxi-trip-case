# Databricks notebook source
# MAGIC %md
# MAGIC ## Análises solicitadas
# MAGIC Este notebook armazena as queries que serão executadas para responder as perguntas designadas ao case. Baseado em como a arquitetura foi desenvolvida, os resultados podem ser obtidos através de diferentes formas dentro da camada gold: consultando os valores diretamente nas tabelas agrupadas ou realizando a query na tabela de consulta. Todas as abordagens serão apresentadas aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Qual a média de valor total (total\_amount) recebido em um mês considerando todos os yellow táxis da frota?
# MAGIC Neste caso, o texto pode ser interpretado de duas formas:
# MAGIC - a. Qual a média mensal do faturamento total dos táxis amarelos.
# MAGIC - b. Qual a média mensal por viagem dos táxis amarelos de um mês selecionados.
# MAGIC
# MAGIC Para isso, apresentaremos os resultados para as duas perguntas por diversas formas de consulta. Neste caso, consideraremos o mês de referência como **pickup_time_year_month** e consideraremos somente viagens de **janeiro** a **maio**

# COMMAND ----------

# MAGIC %md
# MAGIC #### a. Média mensal do faturamento total dos táxis amarelos

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i. Usando a tabela de consulta (tb_gold_taxi_trips_full_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC with tabela_faturamento as (
# MAGIC     SELECT
# MAGIC         pickup_time_year_month,
# MAGIC         SUM(total_amount) as total_revenue
# MAGIC     FROM nyc_taxi.gold.tb_gold_taxi_trips_full_data
# MAGIC     WHERE taxi_color = 'yellow'
# MAGIC     GROUP BY 1
# MAGIC     ORDER BY pickup_time_year_month
# MAGIC )
# MAGIC
# MAGIC SELECT ROUND(AVG(TOTAL_REVENUE),2) AS MEDIA_FATURAMENTO_MENSAL 
# MAGIC FROM tabela_faturamento

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ii. Usando a tabela agregada (tb_gold_taxi_trips_analysis_per_month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC      ROUND(AVG(total_amount),2) AS MEDIA_FATURAMENTO_MENSAL 
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_month
# MAGIC WHERE taxi_color = 'yellow'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Média mensal por viagem de um mês selecionado

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i. Usando a tabela de consulta (tb_gold_taxi_trips_full_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_time_year_month,
# MAGIC     ROUND(AVG(total_amount),2) as MEDIA_FATURAMENTO_POR_VIAGEM
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_full_data
# MAGIC WHERE taxi_color = 'yellow'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ii. Usando a tabela agregada (tb_gold_taxi_trips_analysis_per_month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_time_year_month,
# MAGIC     avg_amount AS MEDIA_FATURAMENTO_POR_VIAGEM
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_month
# MAGIC WHERE taxi_color = 'yellow'
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Qual a média de passageiros (passenger\_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota??
# MAGIC Neste caso, o cálculo pode ser feito de duas maneiras:
# MAGIC - Considerando as viagens que não tiveram registro de passageiros, ou seja, passenger_count é nulo ou 0
# MAGIC - Considerando as viagens que tiveram registro de passageiro diferente de nulos
# MAGIC
# MAGIC Para isso, apresentaremos os resultados para as duas perguntas por diversas formas de consulta. Neste caso, consideraremos o mês de referência como **pickup_time_year_month**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### a. Considerando somente viagens que tiveram pelo menos 1 passageiro registrado

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i. Usando a tabela de consulta (tb_gold_taxi_trips_full_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_time_year_month,
# MAGIC     pickup_time_hour,
# MAGIC     ROUND(avg(passenger_count),2) AS MEDIA_PASSAGEIROS_POR_VIAGEM
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_full_data
# MAGIC WHERE
# MAGIC     pickup_time_year_month = '2023-05' AND
# MAGIC     is_passenger_count_recorded IS TRUE
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1 ASC, 2 ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ii. Usando a tabela agregada (tb_gold_taxi_trips_analysis_per_hour_per_month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_time_year_month,
# MAGIC     pickup_time_hour,
# MAGIC     avg_passengers_count_with_recorded_data AS MEDIA_PASSAGEIROS_POR_VIAGEM
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_hour_per_month
# MAGIC WHERE
# MAGIC     pickup_time_year_month = '2023-05'
# MAGIC ORDER BY 1 ASC, 2 ASC

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Considerando somente viagens que tiveram contagem de passageiros não nula

# COMMAND ----------

# MAGIC %md
# MAGIC ##### i. Usando a tabela de consulta (tb_gold_taxi_trips_full_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_time_year_month,
# MAGIC     pickup_time_hour,
# MAGIC     ROUND(avg(passenger_count),2) AS MEDIA_PASSAGEIROS_POR_VIAGEM
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_full_data
# MAGIC WHERE
# MAGIC     pickup_time_year_month = '2023-05' AND
# MAGIC     passenger_count is not null
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1 ASC, 2 ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ii. Usando a tabela agregada (tb_gold_taxi_trips_analysis_per_hour_per_month)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_time_year_month,
# MAGIC     pickup_time_hour,
# MAGIC     avg_passengers_count_all_data AS MEDIA_PASSAGEIROS_POR_VIAGEM
# MAGIC FROM nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_hour_per_month
# MAGIC WHERE
# MAGIC     pickup_time_year_month = '2023-05'
# MAGIC ORDER BY 1 ASC, 2 ASC

# COMMAND ----------

