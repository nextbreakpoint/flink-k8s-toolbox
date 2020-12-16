#!/bin/sh

dot -Tpng flink-operator.dot > flink-operator.png
dot -Tpng flink-cluster.dot > flink-cluster.png
dot -Tpng flink-job.dot > flink-job.png
