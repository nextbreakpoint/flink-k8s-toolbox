#!/usr/bin/env sh

set -x
set -e

./gradlew integrationTest --info --tests="com.nextbreakpoint.flinkoperator.integration.cases.${1}Test"

