import json
from confluent_kafka import Consumer, Producer, KafkaError


def run(bootstrap_servers: str):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'system-b',
        'auto.offset.reset': 'earliest',
    })
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    consumer.subscribe([
        'customer-email',
        'customer-phone',
        'customer-name',
        'customer-address',
        'customer-city',
        'customer-personal-number',
        'customer-country',
    ])

    store: dict[str, dict] = {}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(msg.error())

            headers = dict(msg.headers() or [])
            customer_id = headers.get('id', b'').decode()

            data = json.loads(msg.value())
            if customer_id not in store:
                store[customer_id] = {}
            store[customer_id].update(data)

            # Skicka växande meddelande för varje nytt fält
            print(f"Fält mottaget för {customer_id}: {list(data.keys())[0]} ({len(store[customer_id])}/7 fält)")
            producer.produce(
                topic='customer-complete',
                key=customer_id,
                value=json.dumps(store[customer_id]),
                headers=[('id', customer_id)],
            )
            producer.flush()

    finally:
        consumer.close()