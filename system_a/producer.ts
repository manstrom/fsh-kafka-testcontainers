import { Kafka, Producer } from 'kafkajs';

export function makeProducer(bootstrapServers: string): Producer {
  const kafka = new Kafka({ brokers: [bootstrapServers] });
  return kafka.producer();
}

export interface CustomerData {
  email: string;
  phone: string;
  name: string;
  address: string;
}

export async function publishCustomer(
  producer: Producer,
  customerId: string,
  customer: CustomerData
): Promise<void> {
  await producer.send({
    topic: 'customer-contact',
    messages: [{
      key: customerId,
      value: JSON.stringify({ email: customer.email, phone: customer.phone }),
      headers: { id: customerId },
    }],
  });

  await producer.send({
    topic: 'customer-identity',
    messages: [{
      key: customerId,
      value: JSON.stringify({ name: customer.name, address: customer.address }),
      headers: { id: customerId },
    }],
  });
}