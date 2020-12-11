#!/bin/sh

java \
    -XX:+UseParallelGC \
    -XX:MinHeapFreeRatio=5 \
    -XX:MaxHeapFreeRatio=10 \
    -XX:GCTimeRatio=4 \
    -XX:AdaptiveSizePolicyWeight=90 \
    -jar \
    /usr/local/bin/flink-k8s-toolbox.jar \
    $@
