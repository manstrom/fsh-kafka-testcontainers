import { Kafka, Producer } from "kafkajs";

// ✅ 1. Define and export type
export type CustomerData = {
  email: string;
  phone: string;
  name: string;
  address: string;
  city: string;
  personalNumber: string;
  country: string;
};

// ✅ 2. Create Kafka producer
export function makeProducer(brokers: string[]): Producer {
  const kafka = new Kafka({ brokers });
  return kafka.producer();
}

// ✅ 3. Generate fake data (simple version)
export function generateCustomerData(): CustomerData {
  return {
    email: "test@example.com",
    phone: "0701234567",
    name: "Test User",
    address: "Test Street 1",
    city: "Stockholm",
    personalNumber: "19900101-1234",
    country: "Sweden",
  };
}

// ✅ 4. Your existing function (unchanged)
export async function publishCustomer(
  producer: Producer,
  customerId: string,
  customer: CustomerData
): Promise<void> {

  await producer.send({
    topic: 'customer-email',
    messages: [{ key: customerId, value: JSON.stringify({ email: customer.email }), headers: { id: customerId } }],
  });

  await producer.send({
    topic: 'customer-phone',
    messages: [{ key: customerId, value: JSON.stringify({ phone: customer.phone }), headers: { id: customerId } }],
  });

  await producer.send({
    topic: 'customer-name',
    messages: [{ key: customerId, value: JSON.stringify({ name: customer.name }), headers: { id: customerId } }],
  });

  await producer.send({
    topic: 'customer-address',
    messages: [{ key: customerId, value: JSON.stringify({ address: customer.address }), headers: { id: customerId } }],
  });

  await producer.send({
    topic: 'customer-city',
    messages: [{ key: customerId, value: JSON.stringify({ city: customer.city }), headers: { id: customerId } }],
  });

  await producer.send({
    topic: 'customer-personal-number',
    messages: [{ key: customerId, value: JSON.stringify({ personalNumber: customer.personalNumber }), headers: { id: customerId } }],
  });

  await producer.send({
    topic: 'customer-country',
    messages: [{ key: customerId, value: JSON.stringify({ country: customer.country }), headers: { id: customerId } }],
  });
}