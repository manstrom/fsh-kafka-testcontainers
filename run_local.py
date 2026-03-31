"""
Kör detta för att hålla Kafka + System A uppe så du kan kolla i Offset Explorer.
Avsluta med Ctrl+C när du är klar.
"""
import time
import threading
from testcontainers.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from confluent_kafka.admin import AdminClient, NewTopic
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))


def create_topics(bootstrap_servers: str):
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    topics = [
        NewTopic('customer-email',        num_partitions=1, replication_factor=1),
        NewTopic('customer-phone',        num_partitions=1, replication_factor=1),
        NewTopic('customer-name',         num_partitions=1, replication_factor=1),
        NewTopic('customer-address',      num_partitions=1, replication_factor=1),
        NewTopic('customer-city',         num_partitions=1, replication_factor=1),
        NewTopic('customer-personal-number', num_partitions=1, replication_factor=1),
        NewTopic('customer-country',      num_partitions=1, replication_factor=1),
        NewTopic('customer-complete',     num_partitions=1, replication_factor=1),
    ]
    admin.create_topics(topics)
    time.sleep(2)


if __name__ == '__main__':
    print("Startar Kafka...")
    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()
        print(f"✅ Kafka kör på: {bootstrap}")
        create_topics(bootstrap)
        print("✅ Topics skapade!")

        # Hitta internt nätverk för System A
        kafka_container = kafka.get_wrapped_container()
        kafka_container.reload()
        networks = kafka_container.attrs['NetworkSettings']['Networks']
        network_name = list(networks.keys())[0]
        kafka_internal_ip = networks[network_name]['IPAddress']
        internal_bootstrap = f"{kafka_internal_ip}:9092"

        print("Startar System A...")
        container = (
            DockerContainer('fsh-system-a')
            .with_env('KAFKA_BOOTSTRAP_SERVERS', internal_bootstrap)
            .with_exposed_ports(8000)
            .with_kwargs(network=network_name)
        )
        with container:
            host = container.get_container_host_ip()
            port = container.get_exposed_port(8000)
            print(f"✅ System A kör på: http://{host}:{port}")
            print()
            print("=" * 50)
            print(f"  Koppla Offset Explorer till: {bootstrap}")
            print(f"  Bootstrap servers: {bootstrap}")
            print("=" * 50)
            print()
            print("Tryck Ctrl+C för att stänga av...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStänger av...")