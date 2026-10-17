import json
import os
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
import polars as pl


os.environ["POLARS_SKIP_CPU_CHECK"] = "1"


conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'polars-traffic-parquet-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
topic_name = 'transito_server.public.tb_acidentes'
consumer.subscribe([topic_name])


BATCH_SIZE = 500
INACTIVITY_TIMEOUT = 5.0
BASE_DIR = Path(__file__).resolve().parent.parent.parent if '__file__' in locals() else Path('.').resolve()
OUTPUT_DIR = BASE_DIR / 'src' / 'raw' / 'data' / 'processed' / 'acidentes_stream'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

batch_buffer = []


def flush_buffer_to_parquet(buffer):
    if not buffer:
        return
    try:
        df = pl.DataFrame(buffer)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = OUTPUT_DIR / f"batch_{timestamp_str}.parquet"
        df.write_parquet(file_path)
        print(f"[PARQUET] Lote com {len(buffer)} registros gravado em: {file_path}")
    except Exception as e:
        print(f"[ERRO] Falha ao gravar lote no Parquet: {e}")
    finally:
        buffer.clear()


print(f"Conectado ao Kafka. Consumindo tópico: {topic_name} até zerar o lag...")

last_message_time = time.time()

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:

            if time.time() - last_message_time > INACTIVITY_TIMEOUT:
                print("\n[INFO] Nenhuma nova mensagem recebida por inatividade. O lag foi zerado.")
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Erro no broker: {msg.error()}")
                break


        last_message_time = time.time()

        raw_value = msg.value()
        if raw_value:
            event_data = json.loads(raw_value.decode('utf-8'))
            payload = event_data.get('payload')

            if payload and payload.get('after'):
                batch_buffer.append(payload['after'])

                if len(batch_buffer) >= BATCH_SIZE:
                    flush_buffer_to_parquet(batch_buffer)

except KeyboardInterrupt:
    print("\nInterrompido manualmente.")


if batch_buffer:
    print("Salvando lote final pendente...")
    flush_buffer_to_parquet(batch_buffer)

consumer.close()
print("Consumidor finalizado com sucesso. Task do Airflow concluída!")