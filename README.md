> ⚠ TODO restructure package; have all statemachine impls in their own packages

# Cluster Startup

For development, 

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

TODO docker-compose!

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