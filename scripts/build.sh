#!/bin/sh

./gradlew build copyRuntimeDeps

source .utils

$GRAALVM_HOME/bin/native-image --verbose -cp $(classpath)
