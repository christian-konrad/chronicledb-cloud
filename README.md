# ChronicleDB on a Raft

[![DOI](https://zenodo.org/badge/394248374.svg)](https://zenodo.org/badge/latestdoi/394248374)

## Build

```
mvn clean
mvn frontend:yarn
mvn frontend:webpack
mnv package
```

### Compile protos after change

```
mvn clean
mnv package
```

Package may fail as generated java classes change after protos are compiled,
but they should be indexed by the IDE regardless.

## Configuration

Either via the `application.properties` or run arguments.

## Cluster Startup

```shell
PEERS=n1:localhost:6000,n2:localhost:6001,n3:localhost:6002
ID=n0
HTTP_PORT=8080
META_PORT=6000
REPLICATION_PORT=6050

./mvnw spring-boot:run -Dspring-boot.run.arguments="\
--node-id={ID} \
--server.address=localhost \
--server.port={HTTP_PORT} \
--metadata-port={META_PORT} \
--replication-port={REPLICATION_PORT} \
--storage=/tmp/raft-demo/{ID} \
--peers={PEERS}"
```

#### Example

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

### From JAR

```shell
java -jar target/raft-log-replication-demo-0.0.1-SNAPSHOT.jar \
--node-id={ID} \
--server.address=localhost \
--server.port={HTTP_PORT} \
--metadata-port={META_PORT} \
--replication-port={REPLICATION_PORT} \
--storage=/tmp/raft-demo/{ID} \
--peers={PEERS}"
```

#### Example Cluster

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

## Docker

See the docker-compose as well as the Dockerfile.

## Provisioning a partition

In general, a new partition of any state machine (any that implements the `StateMachine` interface) can be instantiated using the REST API:

```sh
POST http://localhost:8080/api/cluster-manager/partitions
Content-Type: application/json

{
  "stateMachineClassName": "de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.EventStoreStateMachine",
  "partitionName": "events",
  "replicationFactor": 3
}
```

For event stores, there is a convenience method to instantiate a new stream on its own partition:

```sh
POST http://localhost:8080/api/event-store/streams
Content-Type: application/json

{
  "streamName": "demo-event-store",
  "schema": [
    {
      "name": "SYMBOL",
      "type": "STRING",
      "properties": {}
    },
    {
      "name": "SECURITYTYPE",
      "type": "INTEGER",
      "properties": {}
    },
    {
      "name": "LASTTRADEPRICE",
      "type": "FLOAT",
      "properties": {}
    }
  ]
}
```

> Note that the schema is currently ignored, since it is hardcoded. The implementation of the dynamic schema invocation is currently pending.

## Roadmap

### Make a library out of the Apache Ratis on-top abstractions
One sub-goal is to create a standalone, high-level abstraction of Apache Ratis to enable developers to build high available applications with strong consistent replication in record time.

### Test coverage
There is currently little to no test coverage. We want everything to be tested.