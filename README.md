# Cluster Startup

```shell
PEERS=n0:localhost:6000,n1:localhost:6001,n2:localhost:6002
ID=n0
HTTP_PORT=8080

TODO server.host


./mvnw spring-boot:run -Dspring-boot.run.arguments="\
--node-id={ID} \
--storage=/tmp/raft-demo/{ID} \
--server.port={HTTP_PORT} \
--peers={PEERS}"
```

```shell
./mvnw spring-boot:run -Dspring-boot.run.arguments="\
--node-id=n1 \
--server.port=8080 \
--storage=/tmp/raft-demo/n1 \
--peers=n0:localhost:6000,n1:localhost:6001,n2:localhost:6002"
```