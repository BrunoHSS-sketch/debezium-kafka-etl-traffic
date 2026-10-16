# Debezium Kafka ETL Traffic

Projeto de pipeline ETL para dados de trânsito, usando PostgreSQL, Kafka, Debezium, Airflow e processamento de eventos em Parquet com Polars.

## Visão geral

Este repositório implementa um fluxo de ingestão de dados em que:

- um banco PostgreSQL simula um sistema transacional;
- o Debezium captura mudanças de dados via CDC;
- o Kafka recebe os eventos e os encaminha para consumo;
- um consumidor em Python lê mensagens do Kafka;
- os registros são convertidos em micro-batches e gravados em arquivos Parquet;
- o Airflow orquestra a carga inicial dos CSVs e o consumo do Kafka.

A ideia central é demonstrar um pipeline local de dados com arquitetura moderna para dados de acidentes e trânsito, usando ferramentas populares em projetos de dados em tempo real.

## Estrutura do projeto

```text
.
├── .venv/
├── airflow/
│   ├── dags/
│   │   └── traffic_pipeline_dag.py
│   └── logs/
├── src/
│   ├── consumer/
│   │   └── kafka_polars_consumer.py
│   └── raw/
│       ├── data/
│       │   ├── acidentes2026.csv
│       │   └── datatran2026.csv
│       └── data_injection.py
├── Dockerfile
├── docker-compose.yaml
├── main.py
├── pyproject.toml
├── requirements.txt
├── uv.lock
└── README.md
```

## Componentes principais

### 1. PostgreSQL
O serviço `postgres` é a fonte de dados. Ele roda em um contêiner local com `wal_level=logical`, necessário para que o Debezium funcione corretamente.

### 2. Kafka + Zookeeper
A infraestrutura de streaming é composta por:

- Zookeeper
- Kafka broker
- Kafka Connect

### 3. Debezium
O Kafka Connect usa o Debezium para monitorar o PostgreSQL e publicar eventos de mudança em tópicos Kafka.

### 4. Airflow
O DAG `pipeline_transito_local` orquestra duas etapas:

1. carga inicial dos CSVs na base PostgreSQL;
2. consumo do tópico Kafka e gravação em arquivos Parquet.

### 5. Processamento em Parquet
O consumidor em Python usa `confluent-kafka` e `polars` para:

- ler mensagens do Kafka;
- acumular registros em lote;
- salvar lotes em arquivos `.parquet` na pasta `src/raw/data/processed/...`.

## Pré-requisitos

- Docker
- Docker Compose
- Python 3.12+
- `pip` ou `uv`

## Configuração do ambiente

### 1. Clonar o projeto

```bash
git clone <url-do-repositorio>
cd debzium-kafka-etl-traffic
```

### 2. Criar o ambiente virtual

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Instalar dependências

```bash
pip install -r requirements.txt
```

Ou, se preferir com `uv`:

```bash
uv sync
```

## Subir a infraestrutura local

```bash
docker compose up -d
```

Isso inicia os serviços:

- PostgreSQL
- Zookeeper
- Kafka
- Kafka Connect
- Airflow

## Executar o pipeline

### Carga inicial dos CSVs

```bash
python src/raw/data_injection.py
```

Esse script lê os arquivos CSV localizados em `src/raw/data/` e grava as tabelas `tb_acidentes` e `tb_datatran` no PostgreSQL.

### Consumidor Kafka para Parquet

```bash
python src/consumer/kafka_polars_consumer.py
```

Esse script fica ouvindo o tópico Kafka configurado e salva lotes em arquivos Parquet.

## DAG do Airflow

O DAG está em:

```text
airflow/dags/traffic_pipeline_dag.py
```

Ele executa os comandos:

```bash
python /opt/airflow/src/raw/data_injection.py
python /opt/airflow/src/consumer/kafka_polars_consumer.py
```

## Arquivos importantes

- `docker-compose.yaml`: define a infraestrutura local;
- `pyproject.toml`: configuração do projeto e dependências;
- `src/raw/data_injection.py`: carga inicial de CSV para PostgreSQL;
- `src/consumer/kafka_polars_consumer.py`: processamento de eventos Kafka para Parquet;
- `airflow/dags/traffic_pipeline_dag.py`: orquestração do pipeline.

## Observações

- O projeto foi estruturado para execução local e experimentação de arquitetura CDC/ETL.
- O endpoint de integração e os nomes dos tópicos devem ser ajustados conforme o ambiente real.
- Os arquivos de dados de exemplo ficam em `src/raw/data/` e podem ser substituídos pelas fontes reais do projeto.
