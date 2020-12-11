#!/bin/sh

source .utils

$GRAALVM_HOME/bin/java -agentlib:native-image-agent=config-output-dir=graalvm/bootstrap -cp $(classpath) com.nextbreakpoint.flink.cli.Main bootstrap run --namespace=flink --flink-hostname=$(minikubeIp) --kube-config=$(kubeConfig)  --cluster-name=test --job-name=job-1 --jar-path=flink-jobs.jar --class-name=com.nextbreakpoint.flink.jobs.stream.TestJob --parallelism=1 --argument=--DEVELOP_MODE --argument=disabled
