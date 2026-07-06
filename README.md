# 1. PIPELINE DE DADOS ANALYTICS - NYC TAXI TRIPS

Este documento apresenta a documentação completa do repositório para o consumo, armazenamento, limpeza, padronização e curadoria dos dados de viagens dos táxis de Nova York (Yellow e Green Taxis). O projeto segue as melhores práticas de Engenharia de Dados utilizando o ecossistema Apache Spark dentro do ambiente Databricks.

---

# 2. INSTRUÇÕES DE EXECUÇÃO (DATABRICKS FREE EDITION)

O pipeline foi projetado para ser flexível e agnóstico, com sua orquestração principal centralizada no arquivo principal de execução. 

Siga o passo a passo numerado abaixo para configurar e executar a infraestrutura e os fluxos de dados:

### **2.1. Passo 1: Configuração do Repositório no Workspace**
Para que os arquivos fiquem acessíveis no ambiente Databricks, realize uma das seguintes ações:
- Conectar via Git Folders: Acesse o menu Workspace, clique em Create, selecionando Git Folder. Ao abrir o popup, insira a url https://github.com/rodolfoarmelim/nyc-taxi-trip-case.git no campo Git repository URL e clique em Create Git Folder.

- Importação Manual: Crie uma pasta dentro do seu Workspace de preferência e faça o upload da estrutura completa de arquivos (main, pastas src, config e analysis), preservando fielmente a hierarquia de diretórios.

### **2.2. Passo 2: Inicialização da Infraestrutura (Schema Enforced)**
Antes de processar qualquer dado, é obrigatório criar as estruturas de catálogo, schemas e volumes que garantem o controle estrito de esquemas (Schema Enforcement):
- Localize e abra o arquivo localizado em config/env_creation.py.
- Execute todo o conteúdo do arquivo clicando no botão de Play (Run All) no canto superior direito. 
- Este passo criará o catálogo nyc_taxi, o schema landing com o volume landing_zone, além dos schemas bronze, silver e gold com suas respectivas tabelas vazias prontas para receber cargas eficientes via Delta Lake.

### **2.3. Passo 3: Execução do Pipeline via Interface Gráfica (UI)**
Caso deseje executar todo o pipeline de forma sequencial com as parametrizações padrão:
- Abra o arquivo central main.py no seu ambiente.
- Clique no botão Play (Run) localizado no topo do editor de código do Databricks.
- O pipeline executará todas as etapas sequencialmente para os meses padrão configurados no projeto (janeiro a maio de 2023).

### **2.4. Passo 4: Execução do Pipeline via Terminal Local**
Para executar o pipeline diretamente da sua máquina local utilizando o Databricks Connect (Serverless), é necessário configurar o seu ambiente com as credenciais de acesso à nuvem. Siga o passo a passo abaixo:

**I. Instalação das Dependências**
No terminal databricks, certifique-se de estar na pasta raiz do projeto e instale todos os pacotes necessários:
```bash
pip install -r requirements.txt
```

**II. Como obter o HOST (URL do Workspace)**
1. Abra o seu Databricks no navegador.
2. Copie a URL base que aparece na barra de endereços (ex: `https://adb-123456789.7.azuredatabricks.net` ou `https://dbc-abcdefgh-1234.cloud.databricks.com`).
3. Importante: Copie apenas o domínio principal, removendo qualquer caminho após a primeira barra (`/`).

**III. Como obter o Token de Acesso Pessoal (PAT)**
1. No canto superior direito do Databricks, clique no seu nome de usuário/perfil.
2. Acesse **Settings** (Configurações) e no menu lateral vá em **Developer** (Desenvolvedor).
3. Na seção **Access tokens**, clique em **Manage** e no botão **Generate new token**.
4. Dê um nome para o token e clique em gerar. Copie o código gerado (ele sempre começará com `dapi...`). *Atenção: Salve-o temporariamente, pois ele não será exibido novamente.*

**IV. Configuração das Variáveis no Terminal**
Antes de rodar o código, declare as credenciais no seu terminal para que a sessão do Spark consiga autenticar na nuvem de forma segura. Substitua com os dados obtidos nos passos anteriores:

```bash
export DATABRICKS_HOST="COLOQUE_SUA_URL_AQUI"
export DATABRICKS_TOKEN="COLOQUE_SEU_TOKEN_AQUI"
```

**V. Executando o Pipeline**
Com o ambiente configurado e as credenciais ativas na memória do terminal, inicie o orquestrador com o comando:
```bash
python main.py
```

### **2.5. Passo 5: Variações de Parâmetros e Modos de Execução via Linha de Comando**
O script de orquestração aceita argumentos dinâmicos para permitir reprocessamentos, cargas parciais ou análises de períodos específicos. Veja os parâmetros disponíveis e exemplos práticos de uso em texto corrido:

- Parâmetro --ano-mes: Recebe uma string com os períodos no formato AAAA-MM separados por vírgula. Substitui a execução dos meses padrão.
- Parâmetro --step: Força o pipeline a rodar estritamente um único step lógico (landing, bronze, silver ou gold).
- Parâmetro --start-from: Permite retomar a execução a partir de uma determinada camada, rodando dela até o final do fluxo.

Exemplo de execução focado apenas no download de dados de dois meses específicos:

    python main.py --step landing --ano-mes "2023-01,2023-02"

Exemplo de reprocessamento completo iniciando a partir da camada silver para um único mês de interesse:

    python main.py --start-from silver --ano-mes "2023-05"

---

# 3. ARQUITETURA E REFERÊNCIA DE SCHEMAS

### **3.1. Modelo Arquitetural Medalhão**
O fluxo de processamento de dados adota a Arquitetura Medalhão. Os dados brutos entram por uma zona de pouso temporária e progridem através de três camadas lógicas onde ganham estrutura, qualidade e enrichment analítico, garantindo uma linhagem limpa do dado.

### **3.2. Relação de Tabelas Geradas e Descrições em Português**
O ecossistema do projeto gerencia um total de 6 estruturas principais, divididas entre volumes e tabelas Delta estruturadas da seguinte forma:

#### **3.2.1. Estrutura de Ingestão (Volume)**
- Volume *landing_zone* (Localizado em nyc_taxi.landing.landing_zone): Diretório de armazenamento físico de arquivos brutos no formato Parquet, oriundos diretamente da API pública do NYC TLC.

#### **3.2.2. Camada Bronze (Tabelas de Dados Brutos)**
- Tabela *tb_bronze_green_taxi_trips*: Armazena os dados brutos e históricos das corridas realizadas pelos táxis verdes. Mantém a estrutura original do arquivo original e adiciona metadados de controle. É particionada pela coluna de referência do arquivo de origem.
- Tabela *tb_bronze_yellow_taxi_trips*: Armazena os dados brutos e históricos das corridas dos táxis amarelos, mantendo todas as colunas originais do provedor e particionamento idêntico à tabela verde.

#### **3.2.3. Camada Silver (Tabela de Padronização e Limpeza)**
- Tabela *tb_silver_nyc_taxi_trips*: Tabela unificada e limpa. Consolida os dados históricos de ambas as frotas (Yellow e Green) sob um mesmo contrato de dados, com tipos convertidos, tratamento de nulos executado e remoção de registros inconsistentes. É particionada mensalmente.

#### **3.2.4. Camada Gold (Tabelas Analíticas e de Negócio)**
- Tabela *tb_gold_taxi_trips_full_data*: Tabela refinada de nível granular para consultas ad-hoc abrangentes, contendo os dados limpos enriquecidos com colunas temporais adicionais e carimbo de data/hora de ingestão.
- Tabela *tb_gold_taxi_trips_analysis_per_hour_per_month_full*: Estrutura agregada que calcula métricas de faturamento total, faturamento médio, contagem total de passageiros e médias de passageiros agrupadas por hora do dia e mês de ocorrência.
- Tabela *tb_gold_taxi_trips_analysis_per_month_per_color*: Visão consolidada mensal que divide os indicadores analíticos financeiros e de volumetria por cor do táxi, permitindo a comparação direta de performance entre frotas.

### **3.3. Consulta a Metadados e Dicionário Técnico**
Para análises aprofundadas sobre os tipos exatos de dados de cada coluna (Data Types), comentários nativos da DDL, chaves de partição e mapeamentos completos de campos, consulte o documento técnico de referência disponível na pasta do projeto no caminho: architecture/tables_metadata.md.

---

# 4. ANÁLISES DOS TRATAMENTOS E REGRAS DA CAMADA SILVER

### **4.1. Fundamentação das Regras via Análise Exploratória (EDA)**
Dentro do diretório analysis/, os scripts bronze_to_silver_eda_green.py e bronze_to_silver_eda_yellow.py guardam o histórico detalhado das análises exploratórias que validaram a integridade física e lógica dos dados brutos armazenados na camada Bronze. As últimas células desses arquivos documentam os comportamentos anômalos encontrados na massa de dados e justificam as regras estatísticas e de negócio estritas que foram codificados para a construção de uma camada Silver confiável.

### **4.2. Detalhamento Técnico das Regras Aplicadas e Justificativas**
As transformações e filtros executados durante a transição da camada Bronze para a Silver atuam diretamente no isolamento e na remoção de ruídos de telemetria, erros de preenchimento e inconsistências temporais ou sistêmicas através das seguintes diretrizes:

#### **4.2.1. Remoção de Outliers por Velocidade Média via Análise de Percentis**
- **Regra:** Viagens com velocidades calculadas de forma impossível ou inconsistente foram eliminadas com base no corte estatístico de percentis altos e baixos. O cálculo foi estruturado a partir da razão entre a distância percorrida (trip_distance) e o tempo total decorrido entre o pickup e o dropoff.
- **Justificativa:** A análise exploratória acusou registros com problemas severos de GPS onde distâncias imensas eram computadas em poucos segundos (velocidades acima do percentil 99, ultrapassando limites físicos reais) ou situações em que o veículo constava trafegando a velocidades de jatos comerciais devido a falhas no taxímetro. Inversamente, registros travados com tempo corrido alto e distância estritamente zerada (percentil zero) também foram expurgados para evitar a distorção das métricas de eficiência operacional na camada analítica.

#### **4.2.2. Remoção de Outliers por Taxa Paga por Quilômetro via Análise de Percentis**
- **Regra:** Aplicação de um filtro de corte de percentis na métrica calculada da taxa financeira por unidade de distância (total_amount dividido por trip_distance). Foram removidas do conjunto as viagens cujos valores ficaram abaixo do percentil 1 ou acima do percentil 99.
- **Justificativa:** Identificou-se a presença de dados corrompidos apresentando valores totais astronômicos para distâncias insignificantes (erros de digitação ou fraudes no sistema de bilhetagem que geravam picos artificiais de receita) e, no extremo oposto, viagens longas de dezenas de quilômetros cujo valor final constava como frações de centavos. O isolamento dessas caudas estatísticas por meio dos percentis 1 e 99 garante que os cálculos de receita média e faturamento por quilômetro na camada Gold representem fielmente o cenário econômico real da frota de NYC.

#### **4.2.3. Filtro de Consistência e Sanidade de Duração de Viagem**
- **Regra:** Eliminação de qualquer registro de viagem onde a duração calculada (diferença absoluta entre dropoff_datetime e pickup_datetime) resulte em valores menores ou iguais a zero segundos.
- **Justificativa:** A EDA detectou anomalias críticas no log de telemetria, incluindo registros onde o horário de término (dropoff) constava como anterior ao horário de início (pickup), gerando durações negativas decorrentes de falhas de fuso horário, ajustes manuais incorretos do sistema ou corrupção do arquivo. Viagens com duração exatamente igual a zero segundos (onde o taxímetro foi aberto e fechado instantaneamente devido a desistências imediatas de passageiros) também foram removidas, pois distorcem gravemente as métricas de tempo médio de corrida e ocupação operacional da frota.

#### **4.2.4. Filtro de Limite de Diferença de Dias (Durações Extremas e Anacronismos)**
- **Regra:** Remoção rigorosa de viagens cuja diferença temporal entre o pickup e o dropoff exceda o limite operacional lógico de 1 dia (24 horas), bem como registros cujos anos de ocorrência estejam desalinhados com o período de análise (como anos de 2008, 2015 ou 2019 capturados dentro de arquivos de 2023).
- **Justificativa:** Casos em que a viagem computa múltiplos dias ou semanas de duração indicam problemas operacionais graves, como motoristas que esqueceram de encerrar a corrida no taxímetro ao término do turno ou dispositivos de telemetria que travaram em estado de atividade contínua. Manter essas linhas geraria outliers gigantescos que invalidariam as análises agregadas da camada Gold. Além disso, os anacronismos de anos passados (erros de relógio interno do hardware dos veículos) violam o isolamento temporal das partições mensais processadas pelo pipeline.

#### **4.2.5. Padronização e Alinhamento de Contratos**
- **Regra:** Os campos originais que identificavam os horários de início e fim da viagem possuíam nomenclaturas divergentes entre os datasets das frotas (tpep_pickup_datetime para amarelos e lpep_pickup_datetime para verdes). Ambos foram unificados e renomeados universalmente para pickup_datetime e dropoff_datetime.
- **Justificativa:** Garantir a consistência e a viabilidade técnica de uma operação de união (Union) direta e performática entre os dois fluxos de dados, eliminando a assimetria de esquemas. Adicionalmente, foi injetada a coluna fixa taxi_color contendo os valores literais 'yellow' ou 'green' em cada fluxo para manter a total rastreabilidade da frota de origem na tabela consolidada.

#### **4.2.6. Saneamento Financeiro e Filtro de Valores Negativos**
- **Regra:** Foi aplicado um filtro rígido onde a coluna total_amount deve ser estritamente maior que zero (total_amount > 0).
- **Justificativa:** A análise exploratória revelou registros contendo valores negativos ou zerados, decorrentes de contestações de cobrança, estornos manuais ou viagens de teste do sistema de suporte. Como o objetivo corporativo do pipeline é analisar o faturamento e o fluxo de caixa real das operações em atividade, esses registros não comerciais foram filtrados para blindar os indicadores financeiros de distorções.

#### **4.2.7. Tratamento Estratégico de Contagem de Passageiros**
- **Regra:** Viagens com valor nulo (Null) ou igual a zero na coluna passenger_count foram mantidas no ecossistema, mas ganharam a criação de uma flag de controle booleana chamada is_passenger_count_recorded (assumindo True se a contagem for maior que zero e False caso contrário).
- **Justificativa:** O descarte puro e simples dessas linhas geraria uma perda massiva de dados financeiros valiosos, visto que essas viagens possuíam cobranças legítimas e registros consistentes de total_amount. Com a aplicação da flag, preserva-se a integridade total do faturamento bruto da Silver, ao mesmo tempo em que se fornece à camada Gold um mecanismo seguro para calcular médias populacionais de passageiros sem poluir as estatísticas com registros onde o motorista esqueceu de registrar os ocupantes no painel.

#### **4.2.8. Criação de Atributos de Tempo para Otimização de Consultas**
- **Regra:** Extração automatizada e armazenamento físico dos componentes temporais derivados das colunas principais de timestamp para gerar os novos campos pickup_time_hour, pickup_time_year_month, dropoff_time_hour e dropoff_time_year_month.
- **Justificativa:** Mitigar o custo computacional de varreduras completas em tabelas volumosas. Esses atributos estruturam o particionamento inteligente do Delta Lake e aceleram expressivamente o tempo de resposta das queries de agregação analítica exigidas pelos consumidores de negócio.

---

# 5. EVOLUÇÕES FUTURAS E MELHORIAS

Como planejamento estratégico para aumentar a maturidade de engenharia deste produto de dados, estão mapeadas as seguintes oportunidades de evolução tecnológica:

### **5.1. Implementação de Infraestrutura como Código (IaC) com Terraform**
- Substituir a criação manual ou por scripts SQL contida em env_creation.py por arquivos declarativos de configuração utilizando o provedor oficial do Terraform para Databricks (Databricks Terraform Provider).
- Benefício: Permite versionar, auditar e replicar de forma automatizada toda a infraestrutura de dados (catálogos, schemas, volumes, permissões de acesso e clusters) entre diferentes ambientes de desenvolvimento, homologação e produção (CI/CD).

### **5.2. Arquitetura Modularizada para Integration com Databricks Workflows (Jobs)**
- Refatorar a parametrização do projeto para transformar os passos do pipeline em tarefas isoladas e modulares, preparadas para serem consumidas nativamente pela ferramenta de agendamento Databricks Jobs.
- Benefício: Substitui a execução manual por um fluxo orquestrado automaticamente através de gatilhos temporais (Crontab) ou eventos, utilizando clusters de Job dedicados (Job Clusters), o que reduz drasticamente os custos operacionais de computação e ativa alertas automáticos via e-mail ou Slack em caso de falhas na execução.