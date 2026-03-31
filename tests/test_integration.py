import json
import time
import threading
import requests
import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic


def create_topics(bootstrap_servers: str):
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    topics = [
        NewTopic('customer-email',           num_partitions=1, replication_factor=1),
        NewTopic('customer-phone',           num_partitions=1, replication_factor=1),
        NewTopic('customer-name',            num_partitions=1, replication_factor=1),
        NewTopic('customer-address',         num_partitions=1, replication_factor=1),
        NewTopic('customer-city',            num_partitions=1, replication_factor=1),
        NewTopic('customer-personal-number', num_partitions=1, replication_factor=1),
        NewTopic('customer-country',         num_partitions=1, replication_factor=1),
        NewTopic('customer-complete',        num_partitions=1, replication_factor=1),
    ]
    admin.create_topics(topics)
    time.sleep(2)


# ✅ Använder run_local.py istället för att starta egna containers
@pytest.fixture(scope='module')
def kafka(kafka_env):
    class FakeKafka:
        def get_bootstrap_server(self):
            return kafka_env['KAFKA_BOOTSTRAP']
    return FakeKafka()


@pytest.fixture(scope='module')
def system_a(kafka_env):
    class FakeSystemA:
        def get_container_host_ip(self):
            return kafka_env['SYSTEM_A_HOST']
        def get_exposed_port(self, port):
            return kafka_env['SYSTEM_A_PORT']
    return FakeSystemA()


def consume_updates(bootstrap_servers: str, customer_id: str, timeout: int = 30):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test-consumer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe(['customer-complete'])

    ALL_FIELDS = {'email', 'phone', 'name', 'address', 'city', 'personalNumber', 'country'}
    latest_data = None
    deadline = time.time() + timeout

    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            headers = dict(msg.headers() or [])
            msg_customer_id = headers.get('id', b'').decode()

            if msg_customer_id != customer_id:
                continue

            data = json.loads(msg.value())
            latest_data = data

            print(f"\n  --- Uppdatering mottagen ({len(data)}/7 fält) ---")
            print(f"  Namn         : {data.get('name', '(saknas)')}")
            print(f"  E-post       : {data.get('email', '(saknas)')}")
            print(f"  Telefon      : {data.get('phone', '(saknas)')}")
            print(f"  Adress       : {data.get('address', '(saknas)')}")
            print(f"  Stad         : {data.get('city', '(saknas)')}")
            print(f"  Personnummer : {data.get('personalNumber', '(saknas)')}")
            print(f"  Land         : {data.get('country', '(saknas)')}")

            if ALL_FIELDS.issubset(data.keys()):
                print(f"\n  Alla 7 fält mottagna!")
                break
    finally:
        consumer.close()

    return latest_data


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

    print(f"\n{'='*50}")
    print(f"  Kund skapad med ID: {customer_id}")
    print(f"{'='*50}")

    data = consume_updates(bootstrap, customer_id)

    print(f"\n{'='*50}")
    print(f"  Slutligt meddelande:")
    print(f"{'='*50}")
    print(f"  Namn         : {data.get('name')}")
    print(f"  E-post       : {data.get('email')}")
    print(f"  Telefon      : {data.get('phone')}")
    print(f"  Adress       : {data.get('address')}")
    print(f"  Stad         : {data.get('city')}")
    print(f"  Personnummer : {data.get('personalNumber')}")
    print(f"  Land         : {data.get('country')}")
    print(f"{'='*50}\n")

    assert data['email'] == 'test@example.com'
    assert data['name'] == 'Anna Svensson'
    assert data['phone'] == '0701234567'
    assert data['address'] == 'Kungsgatan 1, Stockholm'
    assert data['city'] == 'Stockholm'
    assert data['personalNumber'] == '900101-1234'
    assert data['country'] == 'Sverige'