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

## How to run

### Prerequisites
- Docker Desktop must be running
- active Python virtual environment

### Virtual environment

Create:
```powershell
python -m venv .venv
```

Activate:
```powershell
& .venv\Scripts\Activate.ps1
```

Deactivate:
```powershell
deactivate
```
### Install dependencies

Python:
```powershell
pip install -r requirements.txt
```

Node:
```powershell
npm install
```

### Build System A Docker image
```powershell
docker build -t fsh-system-a .
```

### Step 1 - Terminal 1
```powershell
python run_local.py
```

You will get a localhost address like `localhost:54809`. Copy it.

### Step 2 - Terminal 2
```powershell
pytest
```

### Step 3 - Offset Explorer

1. Download offset explorer 3.0
2. Add connection, name it what ever you want
3. Paste the localhost address from Step 1 into Bootstrap servers,
4. Leave Zookeeper unchecked
5. Click Add
6. Expand Topics to view messages

Every time you restart `run_local.py` the port changes — update Bootstrap servers in Offset Explorer with the new address.

---

## TESTERS
Sadaq och Moaz


