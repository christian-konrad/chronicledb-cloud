FROM amazoncorretto:11
USER 1000
VOLUME /tmp
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} raft-log-replication-demo.jar
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /raft-log-replication-demo.jar ${0} ${@}"]
