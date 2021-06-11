FROM openjdk:8-jre-slim

RUN mkdir /data && \
    mkdir -p /opt/jars && \
    mkdir -p /log/cleanerLog

COPY ../jars /opt/jars
COPY ./entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

ENV CLEANER_CLASSPATH /opt/jars

ENTRYPOINT ["/opt/entrypoint.sh"]