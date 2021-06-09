FROM openjdk:8-jre-slim
COPY kyuubi-cleaner-1.2.0-SNAPSHOT.jar /opt/kyuubi-cleaner-1.2.0-SNAPSHOT.jar
COPY entrypoint.sh /opt/entrypoint.sh
RUN mkdir /data
ENTRYPOINT ["/opt/entrypoint.sh"]