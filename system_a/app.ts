import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { makeProducer, publishCustomer, generateCustomerData, CustomerData } from './producer';

const app = express();
app.use(express.json());

const brokers = process.env.KAFKA_BOOTSTRAP_SERVERS!.split(",");
const producer = makeProducer(brokers);

producer.connect().then(() => {
  console.log('Producer connected to Kafka');
});

app.post('/api/v1/customers', async (req: Request, res: Response) => {
  const { email, phone, name, address, city, personalNumber, country } = req.body as CustomerData;
  const customerId = uuidv4();

  await publishCustomer(producer, customerId, { email, phone, name, address, city, personalNumber, country });

  res.json({ id: customerId });
});

const PORT = process.env.PORT ?? 8000;
app.listen(PORT, () => console.log('System A running on port ' + PORT));

export default app;