#!/bin/sh

java \
    -XX:+UseParallelGC \
    -XX:MinHeapFreeRatio=5 \
    -XX:MaxHeapFreeRatio=10 \
    -XX:GCTimeRatio=4 \
    -XX:AdaptiveSizePolicyWeight=90 \
    -Dnetworkaddress.cache.ttl=1 \
    -Dnetworkaddress.cache.negative.ttl=1 \
    -jar \
    /flink-k8s-toolbox.jar \
    $@
