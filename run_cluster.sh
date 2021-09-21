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
