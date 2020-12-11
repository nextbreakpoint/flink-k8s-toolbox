#!/bin/sh

source .utils

$GRAALVM_HOME/bin/java -agentlib:native-image-agent=config-output-dir=graalvm/supervisor -cp $(classpath) com.nextbreakpoint.flink.cli.Main supervisor run --namespace=flink --flink-hostname=$(minikubeIp) --cluster-name=test --kube-config=$(kubeConfig) --dry-run
