# fsh-kafka-testcontainers

A sample application which consumes a Kafka queue (JSON) and outputs to another Kafka queue (JSON)

This should simulate a event-driven scenario where an IT system A produces output (JSON) in Kafka queues which is later consumed by IT system B. IT system B produces output (JSON) in another Kafka queue.

## System A (an simulator)

Should have a REST endpoint to create a new customer (eg POST /api/v1/customers).

Should produce a message with id (uuid) in header with email, phone number in JSON format.

Should produce a message with id (uuid) in header with name, address in JSON format.

id (uuid) is same for both messages and represents the customer. Should be used a key in kafka queues.

System A does not need to store anything.

## System B (System Under Test, SUT)

Consumes the output queues from System A and combine the information into one message.
The id used by System A should be reused by System B in header for Kafka queue.

## Testcontainers

As a integration test for System B, a container simulating System A should be started using testcontainers.

Besides a testcontainer for System A (the simulator) there has to be a Kafka container started, which both System A and B connects to.

Once the tests for System B has been exectued, the containers should be stopped.

