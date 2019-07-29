ARG flink_version
ARG scala_version

FROM flink:${flink_version}-hadoop28-scala_${scala_version}-alpine

ARG flink_version
ARG scala_version

ENV FLINK_PATH /opt/flink
ENV FLINK_VERSION $flink_version
ENV SCALA_VERSION $scala_version

USER root

RUN apk update && apk add wget

USER flink

RUN wget --max-redirect=2 -O $FLINK_PATH/lib/guava-23.0.jar https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/hadoop-aws-2.8.3.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.8.3/hadoop-aws-2.8.3.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/hadoop-auth-2.8.3.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/2.8.3/hadoop-auth-2.8.3.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/hadoop-hdfs-2.8.3.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/2.8.3/hadoop-hdfs-2.8.3.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/hadoop-common-2.8.3.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/2.8.3/hadoop-common-2.8.3.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/joda-time-2.5.jar https://repo1.maven.org/maven2/joda-time/joda-time/2.5/joda-time-2.5.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/jackson-databind-2.6.7.1.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.6.7.1/jackson-databind-2.6.7.1.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/jackson-annotations-2.6.7.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.6.7/jackson-annotations-2.6.7.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/jackson-core-2.6.7.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.6.7/jackson-core-2.6.7.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/jackson-dataformat-cbor-2.6.7.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.7/jackson-dataformat-cbor-2.6.7.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/jackson-datatype-joda-2.6.7.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-joda/2.6.7/jackson-datatype-joda-2.6.7.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/aws-java-sdk-core-1.11.360.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.360/aws-java-sdk-core-1.11.360.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/aws-java-sdk-kms-1.11.360.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-kms/1.11.360/aws-java-sdk-kms-1.11.360.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/aws-java-sdk-s3-1.11.360.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.11.360/aws-java-sdk-s3-1.11.360.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/httpcore-4.4.9.jar https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.9/httpcore-4.4.9.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/httpclient-4.5.5.jar https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.5/httpclient-4.5.5.jar
RUN wget --max-redirect=2 -O $FLINK_PATH/lib/metrics-core-4.0.2.jar https://repo1.maven.org/maven2/io/dropwizard/metrics/metrics-core/4.0.2/metrics-core-4.0.2.jar

RUN wget --max-redirect=2 -O $FLINK_PATH/lib/flink-metrics-graphite-$FLINK_VERSION.jar https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-graphite/$FLINK_VERSION/flink-metrics-graphite-$FLINK_VERSION.jar

RUN chown -R flink.flink $FLINK_PATH/lib

