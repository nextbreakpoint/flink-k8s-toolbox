#!/bin/sh

source .utils

$GRAALVM_HOME/bin/java -agentlib:native-image-agent=config-output-dir=graalvm/operator -cp $(classpath) com.nextbreakpoint.flink.cli.Main operator run --namespace=flink --port=4444 --flink-hostname=$(minikubeIp) --kube-config=$(kubeConfig)  --keystore-path=secrets/keystore-operator-api.jks --truststore-path=secrets/truststore-operator-api.jks --keystore-secret=keystore-password --truststore-secret=truststore-password --dry-run
