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
    -jar \
    /maven/$SERVICE_JAR \
    sidecar $@
