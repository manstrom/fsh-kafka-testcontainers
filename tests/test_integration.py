import json
import time
import threading
import requests
import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic


def create_topics(bootstrap_servers: str):
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    topics = [
        NewTopic('customer-contact',  num_partitions=1, replication_factor=1),
        NewTopic('customer-identity', num_partitions=1, replication_factor=1),
        NewTopic('customer-location', num_partitions=1, replication_factor=1),
        NewTopic('customer-complete', num_partitions=1, replication_factor=1),
    ]
    admin.create_topics(topics)
    time.sleep(2)


@pytest.fixture(scope='module')
def kafka():
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture(scope='module')
def system_a(kafka):
    bootstrap = kafka.get_bootstrap_server()
    create_topics(bootstrap)

    kafka_container = kafka.get_wrapped_container()
    kafka_container.reload()
    networks = kafka_container.attrs['NetworkSettings']['Networks']
    network_name = list(networks.keys())[0]
    kafka_internal_ip = networks[network_name]['IPAddress']
    internal_bootstrap = f"{kafka_internal_ip}:9092"

    print(f"\nKafka extern: {bootstrap}")
    print(f"Kafka intern: {internal_bootstrap}")
    print(f"Nätverk: {network_name}")

    container = (
        DockerContainer('fsh-system-a')
        .with_env('KAFKA_BOOTSTRAP_SERVERS', internal_bootstrap)
        .with_exposed_ports(8000)
        .with_kwargs(network=network_name)
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(8000)
        print(f"System A på: {host}:{port}")
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                requests.get(f'http://{host}:{port}/docs', timeout=2)
                print('System A är redo!')
                break
            except Exception:
                time.sleep(1)
        yield container


def consume_one(bootstrap_servers: str, topic: str, timeout: int = 30):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test-consumer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([topic])
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                headers = dict(msg.headers() or [])
                return (
                    json.loads(msg.value()),
                    msg.key().decode(),
                    headers.get('id', b'').decode(),
                )
    finally:
        consumer.close()
    return None, None, None


def test_customer_flow(kafka, system_a):
    bootstrap = kafka.get_bootstrap_server()

    from system_b import consumer as system_b_consumer
    thread = threading.Thread(
        target=system_b_consumer.run,
        args=(bootstrap,),
        daemon=True,
    )
    thread.start()
    time.sleep(3)

    host = system_a.get_container_host_ip()
    port = system_a.get_exposed_port(8000)
    response = requests.post(f'http://{host}:{port}/api/v1/customers', json={
        'email': 'test@example.com',
        'phone': '0701234567',
        'name': 'Anna Svensson',
        'address': 'Kungsgatan 1, Stockholm',
        'city': 'Stockholm',
        'personalNumber': '900101-1234',
        'country': 'Sverige',
    })
    assert response.status_code == 200
    customer_id = response.json()['id']

    data, key, header_id = consume_one(bootstrap, 'customer-complete')

    print(f"\n{'='*50}")
    print(f"  Kund skapad med ID: {customer_id}")
    print(f"{'='*50}")
    print(f"  Kafka-nyckel  : {key}")
    print(f"  Header ID     : {header_id}")
    print(f"  ----------------------------------------")
    print(f"  Namn          : {data.get('name')}")
    print(f"  E-post        : {data.get('email')}")
    print(f"  Telefon       : {data.get('phone')}")
    print(f"  Adress        : {data.get('address')}")
    print(f"  Stad          : {data.get('city')}")
    print(f"  Personnummer  : {data.get('personalNumber')}")
    print(f"  Land          : {data.get('country')}")
    print(f"{'='*50}\n")

    assert key == customer_id
    assert header_id == customer_id
    assert data['email'] == 'test@example.com'
    assert data['name'] == 'Anna Svensson'
    assert data['phone'] == '0701234567'
    assert data['address'] == 'Kungsgatan 1, Stockholm'
    assert data['city'] == 'Stockholm'
    assert data['personalNumber'] == '900101-1234'
    assert data['country'] == 'Sverige'