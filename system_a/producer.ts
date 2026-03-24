import { Kafka, Producer } from 'kafkajs';
import { faker } from '@faker-js/faker/locale/sv';

export function makeProducer(bootstrapServers: string): Producer {
  const kafka = new Kafka({ brokers: [bootstrapServers] });
  return kafka.producer();
}

export interface CustomerData {
  email: string;
  phone: string;
  name: string;
  address: string;
  city: string;
  personalNumber: string;
  country: string;
}

export function generateCustomerData(): CustomerData {
  return {
    email: faker.internet.email(),
    phone: faker.phone.number(),
    name: faker.person.fullName(),
    address: faker.location.streetAddress(),
    city: faker.location.city(),
    personalNumber: faker.date.birthdate({ min: 18, max: 90, mode: 'age' })
                    .toISOString()
                    .slice(2, 10)          // YYMMDD
                    .replace(/-/g, '')
                  + '-'
                  + faker.string.numeric({ length: 4 }), // XXXX
    country: faker.location.country(),
  };
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

  await producer.send({
    topic: 'customer-location',
    messages: [{
      key: customerId,
      value: JSON.stringify({
        city: customer.city,
        personalNumber: customer.personalNumber,
        country: customer.country,
      }),
      headers: { id: customerId },
    }],
  });
}