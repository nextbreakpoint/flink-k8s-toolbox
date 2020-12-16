ARG flink_version
ARG scala_version

FROM flink:${flink_version}-scala_${scala_version}

ARG flink_version
ARG scala_version

ENV FLINK_VERSION $flink_version
ENV SCALA_VERSION $scala_version

RUN cp $FLINK_HOME/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar $FLINK_HOME/lib/flink-s3-fs-hadoop-${FLINK_VERSION}.jar
RUN cp $FLINK_HOME/opt/flink-metrics-graphite-${FLINK_VERSION}.jar $FLINK_HOME/lib/flink-metrics-graphite-${FLINK_VERSION}.jar
RUN cp $FLINK_HOME/opt/flink-metrics-prometheus-${FLINK_VERSION}.jar $FLINK_HOME/lib/flink-metrics-prometheus-${FLINK_VERSION}.jar

COPY flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod a+x /docker-entrypoint.sh