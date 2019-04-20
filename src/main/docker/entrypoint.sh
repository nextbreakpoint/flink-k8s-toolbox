#!/bin/sh

java \
    -XX:+UseParallelGC \
    -XX:MinHeapFreeRatio=5 \
    -XX:MaxHeapFreeRatio=10 \
    -XX:GCTimeRatio=4 \
    -XX:AdaptiveSizePolicyWeight=90 \
    -XX:MaxRAMPercentage=70 \
    -Dnetworkaddress.cache.ttl=1 \
    -Dnetworkaddress.cache.negative.ttl=1 \
    -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.Log4j2LogDelegateFactory \
    -jar \
    /maven/$SERVICE_JAR \
    $@
