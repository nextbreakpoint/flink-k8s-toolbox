#!/bin/sh

dot -Tpng flink-cluster.dot > flink-cluster.png
dot -Tpng task-executor.dot > task-executor.png
