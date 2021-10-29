# Framework for Replicated Services using Apache Ratis

> 🚧 TODO give it a nice, recognizable name!

> 🚧 TODO Explain why, what, how
 
> 🚧 TODO Rename the whole package as it is no longer a "demo"

# Cluster Startup

> 🚧 TODO Clean up, explain best practices for dev, explain using IntelliJ, explain how to build using maven (include yarn and protobuf builds), show simple examples

```shell
PEERS=n1:localhost:6000,n2:localhost:6001,n3:localhost:6002
ID=n0
HTTP_PORT=8080
META_PORT=6000
RAFT_PORT=6050

./mvnw spring-boot:run -Dspring-boot.run.arguments="\
--node-id={ID} \
--server.address=localhost \
--server.port={HTTP_PORT} \
--metadata-port={META_PORT} \
--replication-port={RAFT_PORT} \
--storage=/tmp/raft-demo/{ID} \
--peers={PEERS}"
```

### Example

```shell
./mvnw spring-boot:run -Dspring-boot.run.arguments="\
--node-id=n1 \
--server.address=localhost \
--server.port=8080 \
--metadata-port=6000 \
--replication-port=6050 \
--storage=/tmp/raft-demo/n1 \
--peers=n0:localhost:6000,n1:localhost:6001,n2:localhost:6002"
```

## From JAR

```shell
java -jar target/raft-log-replication-demo-0.0.1-SNAPSHOT.jar \
--node-id={ID} \
--server.address=localhost \
--server.port={HTTP_PORT} \
--metadata-port={META_PORT} \
--replication-port={RAFT_PORT} \
--storage=/tmp/raft-demo/{ID} \
--peers={PEERS}"
```

### Example Cluster

```shell
PEERS=n1:localhost:6000,n2:localhost:6001,n3:localhost:6002

ID=n1
java -jar target/raft-log-replication-demo-0.0.1-SNAPSHOT.jar \
--node-id=${ID} \
--server.address=localhost \
--server.port=8080 \
--metadata-port=6000 \
--replication-port=6050 \
--storage=/tmp/raft-demo/${ID} \
--peers=${PEERS}

ID=n2
java -jar target/raft-log-replication-demo-0.0.1-SNAPSHOT.jar \
--node-id=${ID} \
--server.address=localhost \
--server.port=8081 \
--metadata-port=6001 \
--replication-port=6051 \
--storage=/tmp/raft-demo/${ID} \
--peers=${PEERS}

ID=n3
java -jar target/raft-log-replication-demo-0.0.1-SNAPSHOT.jar \
--node-id=${ID} \
--server.address=localhost \
--server.port=8082 \
--metadata-port=6002 \
--replication-port=6052 \
--storage=/tmp/raft-demo/${ID} \
--peers=${PEERS}
```

# Docker

## Build
Maven build first, then
```shell
docker build --tag=raft-log-replication-demo:latest .
```

```shell
docker run -p {HTTP_PORT}:{HTTP_PORT} raft-log-replication-demo \
--node-id={ID} \
--server.address=localhost \
--server.port={HTTP_PORT} \
--metadata-port={META_PORT} \
--replication-port={RAFT_PORT} \
--storage=/tmp/raft-demo/{ID} \
--peers={PEERS}
```

### Example Cluster

> 🚧 TODO clean up, explain the docker-compose, show simple example

```shell
PEERS=n1:localhost:6000,n2:localhost:6001,n3:localhost:6002

ID=n1
docker run -p 8080:8080 raft-log-replication-demo \
--node-id=${ID} \
--server.address=localhost \
--server.port=8080 \
--metadata-port=6000 \
--replication-port=6050 \
--storage=/tmp/raft-demo/${ID} \
--peers={PEERS}

ID=n2
docker run -p 8080:8080 raft-log-replication-demo \
--node-id=${ID} \
--server.address=localhost \
--server.port=8081 \
--metadata-port=6001 \
--replication-port=6051 \
--storage=/tmp/raft-demo/${ID} \
--peers=${PEERS}

ID=n3
docker run -p 8080:8080 raft-log-replication-demo \
--node-id=${ID} \
--server.address=localhost \
--server.port=8082 \
--metadata-port=6002 \
--replication-port=6052 \
--storage=/tmp/raft-demo/${ID} \
--peers=${PEERS}
```

> 🚧 TODO Architecture Diagram

## Roadmap - Pending TODOs

### Package restructuring
Have all statemachine impls / applications in their own packages

The package structure may then look like

- applications
    - <APPLICATION_NAME>
        - api
        - impl
            - data
            - messages
                - executors
            - <APPLICATION_NAME>StateMachineProvider
            - <APPLICATION_NAME>StateMachine
            - <APPLICATION_NAME>Client

### Make a library out of the Apache Ratis on-top abstractions
The goal is to create a standalone, high-level abstraction of Apache Ratis to enable developers to build high available applications with strong consistent replication in record time.

### Make it deployable on Kubernetes
To make advantage of the failover and replica mechanisms of kubernetes, the management server is to be extended to automatic cluster change and balancing strategies. The goal is that once a node fails and Kubernetes spawns a new one, the Raft service automatically recognizes this and uses the new node in new partitions and those missing a required replica.

### Test coverage
There is currently little to no test coverage. We want everything to be tested.