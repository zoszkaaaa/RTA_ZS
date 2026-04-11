from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []

    # R1: amount > 3000 → +3
    if tx['amount'] > 3000:
        score += 3
        rules.append("R1")

    # R2: elektronika i amount > 1500 → +2
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append("R2")

    # R3: godzina < 6 → +2
    # najpierw spróbuj użyć pola hour, a jeśli go nie ma, wyciągnij godzinę z timestamp
    if 'hour' in tx:
        hour = tx['hour']
    else:
        hour = datetime.fromisoformat(tx['timestamp']).hour

    if hour < 6:
        score += 2
        rules.append("R3")

    return score, rules

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)

    tx['score'] = score
    tx['rules'] = rules

    if score >= 3:
        alert_producer.send('alerts', value=tx)
        print(f"ALERT: {tx['tx_id']} | score={score} | rules={rules}")

alert_producer.flush()
alert_producer.close()
