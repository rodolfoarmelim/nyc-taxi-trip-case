# Documentação do Dicionário de Dados

## Objetivo
Este documento serve como a documentação oficial do modelo de dados do projeto de análise de viagens de táxi da cidade de Nova York (NYC Taxi Trips). Ele detalha a estrutura de metadados das tabelas organizadas nas camadas **Bronze**, **Silver** e **Gold** da arquitetura medalhão. O objetivo principal é fornecer transparência e entendimento sobre as tabelas e seus campos, tipos de dados e o significado de cada coluna tanto no formato original quanto traduzidos para o português, facilitando a governança dos dados e o consumo analítico.

---

## Camada BRONZE

### Tabela: `tb_bronze_green_taxi_trips`

> **Descrição Original:** Table that store raw data of green taxi trips from files stored in landing.landing_zone volume. The archives have standard names of green_tripdata_YYYY-MM.parquet, using the year-month reference of the file to create a year_month_file column to partion the data
>
> **Descrição em Português:** Tabela que armazena os dados brutos das viagens de táxi verde a partir de arquivos armazenados no volume `landing.landing_zone`. Os arquivos possuem nomes padrão como `green_tripdata_AAAA-MM.parquet`, usando a referência de ano-mês do arquivo para criar uma coluna `year_month_file` para particionar os dados.
>
> **Coluna de Particionamento:** `year_month_file`

| Nome da Coluna (`column_name`) | Tipo de Dado (`data_type`) | Descrição Original (`COMMENT`) | Descrição em Português |
| --- | --- | --- | --- |
| `VendorID` | `INT` | A code indicating the LPEP provider that provided the record.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc | Um código indicando o provedor LPEP que forneceu o registro.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc |
| `lpep_pickup_datetime` | `TIMESTAMP` | The date and time when the meter was engaged. | A data e hora em que o taxímetro foi acionado (início da viagem). |
| `lpep_dropoff_datetime` | `TIMESTAMP` | The date and time when the meter was disengaged. | A data e hora em que o taxímetro foi desligado (fim da viagem). |
| `store_and_fwd_flag` | `STRING` | This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka “store and forward,” because the vehicle did not have a connection to the server. | Flag que indica se o registro da viagem foi mantido na memória do veículo antes de ser enviado ao provedor ("armazenar e encaminhar") por falta de conexão com o servidor. |
| `RatecodeID` | `STRING` | The final rate code in effect at the end of the trip.<br><br>1 = Standard rate<br>2 = JFK<br>3 = Newark<br>4 = Nassau or Westchester<br>5 = Negotiated fare<br>6 = Group ride<br>99 = Null/unknown | O código da tarifa final em vigor no final da viagem.<br><br>1 = Tarifa padrão<br>2 = JFK<br>3 = Newark<br>4 = Nassau ou Westchester<br>5 = Tarifa negociada<br>6 = Viagem em grupo<br>99 = Nulo/desconhecido |
| `PULocationID` | `INT` | TLC Taxi Zone in which the taximeter was engaged. | Zona de Táxi TLC onde o taxímetro foi acionado (ponto de embarque). |
| `DOLocationID` | `INT` | TLC Taxi Zone in which the taximeter was disengaged. | Zona de Táxi TLC onde o taxímetro foi desligado (ponto de desembarque). |
| `passenger_count` | `INT` | The number of passengers in the vehicle. | O número de passageiros no veículo. |
| `trip_distance` | `DOUBLE` | The elapsed trip distance in miles reported by the taximeter | A distância percorrida na viagem, em milhas, informada pelo taxímetro. |
| `fare_amount` | `DOUBLE` | The time-and-distance fare calculated by the meter. For additional information on the following columns, see https://www.nyc.gov/site/tlc/passengers/taxi-fare.page | A tarifa de tempo e distância calculada pelo taxímetro. |
| `extra` | `DOUBLE` | Miscellaneous extras and surcharges. | Extras e sobretaxas diversas. |
| `mta_tax` | `DOUBLE` | Tax that is automatically triggered based on the metered rate in use. | Imposto acionado automaticamente com base na tarifa em uso. |
| `tip_amount` | `DOUBLE` | Tip amount – This field is automatically populated for credit card tips. Cash tips are not included. | Valor da gorjeta – Campo preenchido automaticamente para gorjetas de cartão de crédito. Gorjetas em dinheiro não estão incluídas. |
| `tolls_amount` | `DOUBLE` | Total amount of all tolls paid in trip. | Valor total de todos os pedágios pagos na viagem. |
| `ehail_fee` | `DOUBLE` | ehail fee | Taxa do e-hail (aplicativo). |
| `improvement_surcharge` | `DOUBLE` | Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015. | Sobretaxa de melhoria avaliada no início da viagem. Começou a ser cobrada em 2015. |
| `total_amount` | `DOUBLE` | The total amount charged to passengers. Does not include cash tips | O valor total cobrado aos passageiros. Não inclui gorjetas em dinheiro. |
| `payment_type` | `INT` | A numeric code signifying how the passenger paid for the trip. | Um código numérico indicando como o passageiro pagou pela viagem. |
| `trip_type` | `INT` | A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver.<br><br>1 = Street-hail<br>2 = Dispatch | Código indicando se a viagem foi por chamada na rua ou por despacho de frota.<br><br>1 = Chamada na rua<br>2 = Despacho |
| `congestion_surcharge` | `DOUBLE` | Total amount collected in trip for NYS congestion surcharge | Valor total arrecadado na viagem para a sobretaxa de congestionamento do estado de Nova York. |
| `dt_ingestion` | `TIMESTAMP` | Datetime of the data ingestion | Data e hora da ingestão dos dados. |
| `year_month_file` | `STRING` | Year and month on YYYY-MM format of the file the data as collected. Used to partition the data | Ano e mês no formato AAAA-MM extraídos do arquivo coletado. Usado para particionar os dados. |

---

### Tabela: `tb_bronze_yellow_taxi_trips`

> **Descrição Original:** Table that store raw data of yellow taxi trips from files stored in landing.landing_zone volume. The archives have standard names of yellow_tripdata_YYYY-MM.parquet, using the year-month reference of the file to create a year_month_file column to partion the data.
>
> **Descrição em Português:** Tabela que armazena os dados brutos das viagens de táxi amarelo a partir de arquivos armazenados no volume `landing.landing_zone`. Os arquivos possuem nomes padrão como `yellow_tripdata_AAAA-MM.parquet`, usando a referência de ano-mês do arquivo para criar uma coluna `year_month_file` para particionar os dados.
>
> **Coluna de Particionamento:** `year_month_file`

| Nome da Coluna (`column_name`) | Tipo de Dado (`data_type`) | Descrição Original (`COMMENT`) | Descrição em Português |
| --- | --- | --- | --- |
| `VendorID` | `INT` | A code indicating the TPEP provider that provided the record.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc<br>7 = Helix | Um código indicando o provedor TPEP que forneceu o registro.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc<br>7 = Helix |
| `tpep_pickup_datetime` | `TIMESTAMP` | The date and time when the meter was engaged. | A data e hora em que o taxímetro foi acionado (início da viagem). |
| `tpep_dropoff_datetime` | `TIMESTAMP` | The date and time when the meter was disengaged. | A data e hora em que o taxímetro foi desligado (fim da viagem). |
| `passenger_count` | `INT` | The number of passengers in the vehicle. | O número de passageiros no veículo. |
| `trip_distance` | `DOUBLE` | The elapsed trip distance in miles reported by the taximeter | A distância percorrida na viagem, em milhas, informada pelo taxímetro. |
| `RatecodeID` | `STRING` | The final rate code in effect at the end of the trip.<br><br>1 = Standard rate<br>2 = JFK<br>3 = Newark<br>4 = Nassau or Westchester<br>5 = Negotiated fare<br>6 = Group ride<br>99 = Null/unknown | O código da tarifa final em vigor no final da viagem.<br><br>1 = Tarifa padrão<br>2 = JFK<br>3 = Newark<br>4 = Nassau/Westchester<br>5 = Negociada<br>6 = Viagem em grupo<br>99 = Nulo/Desconhecido |
| `store_and_fwd_flag` | `STRING` | This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka “store and forward,” because the vehicle did not have a connection to the server.<br><br>Y = store and forward trip<br>N = not a store and forward trip | Flag que indica se o registro foi mantido na memória do veículo antes de ser enviado ao provedor por falta de conexão.<br><br>Y = Viagem armazenada e encaminhada<br>N = Viagem não armazenada e encaminhada |
| `PULocationID` | `INT` | TLC Taxi Zone in which the taximeter was engaged. | Zona de Táxi TLC onde o taxímetro foi acionado (ponto de embarque). |
| `DOLocationID` | `INT` | TLC Taxi Zone in which the taximeter was disengaged. | Zona de Táxi TLC onde o taxímetro foi desligado (ponto de desembarque). |
| `payment_type` | `INT` | A numeric code signifying how the passenger paid for the trip.<br><br>0 = Flex Fare trip<br>1 = Credit card<br>2 = Cash<br>3 = No charge<br>4 = Dispute<br>5 = Unknown<br>6 = Voided trip | Código indicando como o passageiro pagou pela viagem.<br><br>0 = Tarifa Flexível<br>1 = Cartão de crédito<br>2 = Dinheiro<br>3 = Sem cobrança<br>4 = Disputa<br>5 = Desconhecido<br>6 = Viagem anulada |
| `fare_amount` | `DOUBLE` | The time-and-distance fare calculated by the meter. For additional information on the following columns, see https://www.nyc.gov/site/tlc/passengers/taxi-fare.page | A tarifa de tempo e distância calculada pelo taxímetro. |
| `extra` | `DOUBLE` | Miscellaneous extras and surcharges. | Extras e sobretaxas diversas. |
| `mta_tax` | `DOUBLE` | Tax that is automatically triggered based on the metered rate in use. | Imposto acionado automaticamente com base na tarifa em uso. |
| `tip_amount` | `DOUBLE` | Tip amount – This field is automatically populated for credit card tips. Cash tips are not included. | Valor da gorjeta – Campo preenchido automaticamente para cartões. Gorjetas em dinheiro não incluídas. |
| `tolls_amount` | `DOUBLE` | Total amount of all tolls paid in trip. | Valor total de todos os pedágios pagos na viagem. |
| `improvement_surcharge` | `DOUBLE` | Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015. | Sobretaxa de melhoria avaliada no início da viagem (implantada em 2015). |
| `total_amount` | `DOUBLE` | The total amount charged to passengers. Does not include cash tips | O valor total cobrado aos passageiros. Não inclui gorjetas em dinheiro. |
| `congestion_surcharge` | `DOUBLE` | Total amount collected in trip for NYS congestion surcharge | Valor total arrecadado para a sobretaxa de congestionamento de NY. |
| `airport_fee` | `DOUBLE` | For pick up only at LaGuardia and John F. Kennedy Airports. | Taxa cobrada apenas para embarques nos aeroportos LaGuardia e John F. Kennedy. |
| `dt_ingestion` | `TIMESTAMP` | Datetime of the data ingestion | Data e hora da ingestão dos dados. |
| `year_month_file` | `STRING` | Year and month on YYYY-MM format of the file the data as collected. Used to partition the data | Ano e mês no formato AAAA-MM do arquivo coletado. Usado para particionar os dados. |

---

## Camada SILVER

### Tabela: `tb_silver_taxi_trips`

> **Descrição Original:** Refined silver table containing unified yellow and green taxi trip data. Records cleaned of statistical outliers and adjusted to business rules.
>
> **Descrição em Português:** Tabela Silver refinada contendo dados unificados das viagens de táxis amarelos e verdes. Registros limpos de outliers estatísticos e ajustados às regras de negócios.
>
> **Coluna de Particionamento:** `year_month_file`

| Nome da Coluna (`column_name`) | Tipo de Dado (`data_type`) | Descrição Original (`COMMENT`) | Descrição em Português |
| --- | --- | --- | --- |
| `vendor_id` | `INT` | A code indicating the LPEP provider that provided the record.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc | Um código indicando o provedor que forneceu o registro (LPEP/TPEP).<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc |
| `passenger_count` | `INT` | The number of passengers in the vehicle. | O número de passageiros no veículo. |
| `total_amount` | `DOUBLE` | The total amount charged to passengers. Does not include cash tips | O valor total cobrado aos passageiros. Não inclui gorjetas em dinheiro. |
| `pickup_datetime` | `TIMESTAMP` | The date and time when the meter was engaged. | A data e hora em que o taxímetro foi acionado (início da viagem). |
| `dropoff_datetime` | `TIMESTAMP` | The date and time when the meter was disengaged. | A data e hora em que o taxímetro foi desligado (fim da viagem). |
| `taxi_color` | `STRING` | Identifier for the taxi color: yellow or green. | Identificador para a cor do táxi: yellow (amarelo) ou green (verde). |
| `trip_distance` | `DOUBLE` | The elapsed trip distance in miles reported by the taximeter | A distância percorrida na viagem, em milhas, informada pelo taxímetro. |
| `pickup_time_hour` | `INT` | The hour component extracted from the pickup_datetime. | O componente de hora extraído da data de início da viagem. |
| `pickup_time_year_month` | `STRING` | The year and month component extracted from the pickup datetime. | O componente de ano e mês (AAAA-MM) extraído da data de início da viagem. |
| `dropoff_time_hour` | `INT` | The hour component extracted from the dropoff_datetime. | O componente de hora extraído da data final da viagem. |
| `dropoff_time_year_month` | `STRING` | The year and month component extracted from the dropoff datetime. | O componente de ano e mês (AAAA-MM) extraído da data final da viagem. |
| `is_passenger_count_recorded` | `BOOLEAN` | Flag indicating if the passenger_count is greater than zero and not null. | Flag indicando se a contagem de passageiros é maior que zero e não é nula. |
| `dt_ingestion` | `TIMESTAMP` | Datetime of the data ingestion into the silver layer. | Data e hora da ingestão dos dados na camada Silver. |
| `year_month_file` | `STRING` | Year and month on YYYY-MM format of the file the data as collected. Used to partition the data. | Ano e mês no formato AAAA-MM originado do nome do arquivo. Usado para particionamento. |

---

## Camada GOLD

### Tabela: `tb_gold_taxi_trips_analysis_per_month_per_color`

> **Descrição Original:** Refined gold table containing yellow and green taxi trip data for grouped analysis per month and per color. Stores by the combination of taxi color and the year and month of the pickup_time insightful data about the amount charged (total and average) and number of passengers carried (total, average per trip considering only trips with recorded passenger count and average per trip considering all trips).
>
> **Descrição em Português:** Tabela Gold refinada contendo dados para análises agrupadas por mês e por cor. Armazena a combinação da cor do táxi com o ano/mês da viagem, fornecendo métricas de valores cobrados (total e média) e passageiros transportados (total, média considerando viagens com registro, e média considerando todas as viagens).
>
> **Coluna de Particionamento:** `dropoff_time_year_month`

| Nome da Coluna (`column_name`) | Tipo de Dado (`data_type`) | Descrição Original (`COMMENT`) | Descrição em Português |
| --- | --- | --- | --- |
| `taxi_color` | `STRING` | Identifier for the taxi color: yellow or green. | Identificador para a cor do táxi: yellow (amarelo) ou green (verde). |
| `total_trips` | `INT` | Count of all trips taken in the universe of analysis. | Contagem de todas as viagens realizadas no universo de análise. |
| `total_amount_sum` | `DOUBLE` | Sum of all the amount charged of all trips taken in the universe of analysis. | Soma de todos os valores cobrados nas viagens realizadas no universo de análise. |
| `avg_amount` | `DOUBLE` | Average amount charged per trip in the universe of analysis. | Valor médio cobrado por viagem no universo de análise. |
| `total_passengers` | `INT` | Full amount of passengers carried in all the trips in the universe of analysis. | Quantidade total de passageiros transportados em todas as viagens no universo de análise. |
| `avg_passengers_count_with_recorded_data` | `DOUBLE` | Average number of passengers carried per trip in the universe of analysis considering only the trips with passager count recorded (null or different of zero). | Média de passageiros por viagem no universo de análise considerando apenas registros onde a contagem é maior que zero (não nula). |
| `avg_passengers_count_all_data` | `DOUBLE` | Average number of passengers carried per trip in the universe of analysis considering all trip excluding null values (trips without any passenger_count will not cloud the data) | Média de passageiros por viagem no universo de análise excluindo registros vazios, mas mantendo zeros na conta total. |
| `dt_ingestion` | `TIMESTAMP` | Datetime of the data ingestion into the gold layer. | Data e hora da ingestão dos dados na camada Gold. |
| `dropoff_time_year_month` | `STRING` | The year and month component extracted from the dropoff datetime. | O componente de ano e mês extraído da data/hora de fim da viagem (usado como partição). |

---

### Tabela: `tb_gold_taxi_trips_analysis_per_hour_per_month_full`

> **Descrição Original:** Refined gold table containing unified yellow and green taxi trip data for grouped analysis. Stores by the combination of hour of the pickup_time and the year and month of the pickup_time insightful data about the amount charged (total and average) and number of passengers carried (total, average per trip considering only trips with recorded passenger count and average per trip considering all trips).
>
> **Descrição em Português:** Tabela Gold contendo dados unificados para análises agrupadas. Armazena pela combinação de hora e ano/mês insights detalhados de faturamento e volume de passageiros.
>
> **Coluna de Particionamento:** `dropoff_time_year_month`

| Nome da Coluna (`column_name`) | Tipo de Dado (`data_type`) | Descrição Original (`COMMENT`) | Descrição em Português |
| --- | --- | --- | --- |
| `dropoff_time_hour` | `INT` | The hour component extracted from the dropfoo datetime. | A hora exata extraída do momento de encerramento da viagem. |
| `total_trips` | `INT` | Count of all trips taken in the universe of analysis. | Contagem de todas as viagens realizadas no universo de análise. |
| `total_amount_sum` | `DOUBLE` | Sum of all the amount charged of all trips taken in the universe of analysis. | Soma de todos os valores cobrados nas viagens realizadas no universo de análise. |
| `avg_amount` | `DOUBLE` | Average amount charged per trip in the universe of analysis. | Valor médio cobrado por viagem no universo de análise. |
| `total_passengers` | `INT` | Full amount of passengers carried in all the trips in the universe of analysis. | Quantidade total de passageiros transportados em todas as viagens no universo de análise. |
| `avg_passengers_count_with_recorded_data` | `DOUBLE` | Average number of passengers carried per trip in the universe of analysis considering only the trips with passager count recorded (null or different of zero). | Média de passageiros por viagem no universo de análise considerando apenas registros onde a contagem é maior que zero (não nula). |
| `avg_passengers_count_all_data` | `DOUBLE` | Average number of passengers carried per trip in the universe of analysis considering all trip excluding null values (trips without any passenger_count will not cloud the data) | Média de passageiros por viagem no universo de análise excluindo registros vazios. |
| `dt_ingestion` | `TIMESTAMP` | Datetime of the data ingestion into the gold layer. | Data e hora da ingestão dos dados na camada Gold. |
| `dropoff_time_year_month` | `STRING` | The year and month component extracted from the dropoff datetime. | O componente de ano e mês extraído da data/hora de fim da viagem (usado como partição). |

---

### Tabela: `tb_gold_taxi_trips_full_data`

> **Descrição Original:** Refined gold table containing unified yellow and green taxi trip data. Records cleaned of statistical outliers and adjusted to business rules and only with columns required by business reasons and spcific analysis requirementes.
>
> **Descrição em Português:** Tabela Gold refinada contendo dados unificados de viagens de táxi amarelo e verde. Registros limpos de outliers estatísticos, ajustados para as regras de negócio mantendo apenas colunas estritamente requisitadas para as análises.
>
> **Coluna de Particionamento:** `dropoff_time_year_month`

| Nome da Coluna (`column_name`) | Tipo de Dado (`data_type`) | Descrição Original (`COMMENT`) | Descrição em Português |
| --- | --- | --- | --- |
| `vendor_id` | `INT` | A code indicating the LPEP provider that provided the record.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc | Um código indicando o provedor que forneceu o registro.<br><br>1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc |
| `passenger_count` | `INT` | The number of passengers in the vehicle. | O número de passageiros no veículo. |
| `total_amount` | `DOUBLE` | The total amount charged to passengers. Does not include cash tips | O valor total cobrado aos passageiros. Não inclui gorjetas em dinheiro. |
| `pickup_datetime` | `TIMESTAMP` | The date and time when the meter was engaged. | A data e hora em que o taxímetro foi acionado (início da viagem). |
| `dropoff_datetime` | `TIMESTAMP` | The date and time when the meter was disengaged. | A data e hora em que o taxímetro foi desligado (fim da viagem). |
| `taxi_color` | `STRING` | Identifier for the taxi color: yellow or green. | Identificador para a cor do táxi: yellow (amarelo) ou green (verde). |
| `dropoff_time_hour` | `INT` | The hour component extracted from the dropoff datetime. | O componente de hora extraído da data de término da viagem. |
| `is_passenger_count_recorded` | `BOOLEAN` | Flag indicating if the passenger_count is greater than zero and not null. | Flag indicando se a contagem de passageiros é maior que zero e não nula. |
| `dt_ingestion` | `TIMESTAMP` | Datetime of the data ingestion into the gold layer. | Data e hora da ingestão dos dados na camada Gold. |
| `dropoff_time_year_month` | `STRING` | The year and month component extracted from the dropoff datetime. | O componente de ano e mês extraído da data de término da viagem (usado como partição). |