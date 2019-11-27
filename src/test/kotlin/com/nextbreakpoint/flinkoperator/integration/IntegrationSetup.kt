package com.nextbreakpoint.flinkoperator.integration

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import java.lang.ProcessBuilder.Redirect
import java.time.Duration
import java.util.Date
import java.util.concurrent.TimeUnit
import kotlin.test.fail

open class IntegrationSetup {
    companion object {
        val redirect = Redirect.INHERIT

        val version = "1.2.2-beta"

        val namespace = "integration-${System.currentTimeMillis()}"

        private val skipDockerImages = System.getProperty("skipDockerImages", "false")

        @BeforeAll
        @JvmStatic
        fun printInfo() {
            println("Run test - ${Date()}")
            println("Namespace = $namespace")
            println("Version = $version")
        }

        @BeforeAll
        @JvmStatic
        fun createNamespace() {
            if (createNamespace(redirect = redirect, namespace = namespace) != 0) {
                fail("Can't create namespace")
            }
        }

        @BeforeAll
        @JvmStatic
        fun buildDockerImages() {
            if (skipDockerImages == "true") {
                return
            }
            println("Building operator image...")
            if (buildDockerImage(redirect = redirect, path = ".", name = "integration/flink-k8s-toolbox:$version", args = emptyList()) != 0) {
                fail("Can't build operator image")
            }
            println("Building flink image...")
            val flinkBuildArgs = listOf(
                "--build-arg", "flink_version=1.9.0", "--build-arg", "scala_version=2.11"
            )
            if (buildDockerImage(redirect = redirect, path = "example/flink", name = "integration/flink:1.9.0", args = flinkBuildArgs) != 0) {
                fail("Can't build flink image")
            }
            println("Building job image...")
            val jobBuildArgs = listOf(
                "--build-arg", "repository=integration/flink-k8s-toolbox", "--build-arg", "version=$version"

            )
            if (buildDockerImage(redirect = redirect, path = "example/flink-jobs", name = "integration/flink-jobs:1", args = jobBuildArgs) != 0) {
                fail("Can't build job image")
            }
            println("Images created")
        }

        @BeforeAll
        @JvmStatic
        fun installOperator() {
            println("Installing operator...")
            if (installHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-crd") != 0) {
                if (upgradeHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-crd") != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            val args = listOf(
                /*"--set", "namespace=$namespace",*/
                "--set", "image.pullPolicy=Never",
                "--set", "image.repository=integration/flink-k8s-toolbox",
                "--set", "image.version=$version"
            )
            if (installHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-operator", args = args) != 0) {
                if (upgradeHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-operator", args = args) != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            println("Operator installed")
            println("Starting operator...")
            if (scaleOperator(redirect = redirect, namespace = namespace, replicas = 1) != 0) {
                fail("Can't scale the operator")
            }
            awaitUntilAsserted(timeout = 60) {
                assertThat(isOperatorRunning(redirect, namespace = namespace)).isTrue()
            }
            println("Operator started")
        }

        @BeforeAll
        @JvmStatic
        fun installResources() {
            println("Install resources...")
            if (createResources(redirect = redirect, namespace = namespace, path = "example/config-map.yaml") != 0) {
                if (replaceResources(redirect = redirect, namespace = namespace, path = "example/config-map.yaml") != 0) {
                    fail("Can't create resources")
                }
            }
            if (createResources(redirect = redirect, namespace = namespace, path = "example/secrets.yaml") != 0) {
                if (replaceResources(redirect = redirect, namespace = namespace, path = "example/secrets.yaml") != 0) {
                    fail("Can't create resources")
                }
            }
            println("Resources installed")
        }

        @AfterAll
        @JvmStatic
        fun uninstallOperator() {
            TimeUnit.SECONDS.sleep(10)
            val redirect = Redirect.INHERIT
            println("Stopping operator...")
            if (scaleOperator(redirect = redirect, namespace = namespace, replicas = 0) != 0) {
                println("Can't scale the operator")
            }
            Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until {
                    !isOperatorRunning(redirect = redirect, namespace = namespace)
                }
            println("Operator terminated")
            println("Uninstalling operator...")
            if (uninstallHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-operator") != 0) {
                println("Can't uninstall Helm chart")
            }
            if (uninstallHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-crd") != 0) {
                println("Can't uninstall Helm chart")
            }
            println("Operator uninstalled")
        }

        @AfterAll
        @JvmStatic
        fun deleteNamespace() {
            if (deleteNamespace(redirect = redirect, namespace = namespace) != 0) {
                println("Can't delete namespace")
            }
        }

        fun awaitUntilAsserted(timeout: Long, assertion: () -> Unit) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(5))
                .untilAsserted(assertion)
        }

        fun clusterExists(redirect: Redirect?, namespace: String, name: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc -o json | jq --exit-status -r '.items[] | select(.metadata.name == \"$name\")' >/dev/null"
            )
            return executeCommand(redirect, command) == 0
        }

        fun hasClusterStatus(redirect: Redirect?, namespace: String, name: String, status: ClusterStatus): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc $name -o json | jq --exit-status -r '.status | select(.clusterStatus == \"$status\")' >/dev/null"
            )
            return executeCommand(redirect, command) == 0
        }

        fun createCluster(redirect: Redirect?, namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "create",
                "-f",
                path
            )
            return executeCommand(redirect, command)
        }

        fun deleteCluster(redirect: Redirect?, namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "delete",
                "-f",
                path
            )
            return executeCommand(redirect, command)
        }

        private fun isOperatorRunning(redirect: Redirect?, namespace: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get pod --selector app=flink-operator -o json | jq --exit-status -r '.items[0].status.containerStatuses[] | select(.ready == true)' >/dev/null"
            )
            return executeCommand(redirect, command) == 0
        }

        private fun scaleOperator(redirect: Redirect?, namespace: String, replicas: Int): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "scale",
                "deployment",
                "flink-operator",
                "--replicas",
                "$replicas"
            )
            return executeCommand(redirect, command)
        }

        private fun installHelmChart(redirect: Redirect?, namespace: String, name: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "helm",
                "install",
                "--name",
                "$name",
                "--namespace",
                namespace,
                "helm/$name"
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(redirect, command)
        }

        private fun upgradeHelmChart(redirect: Redirect?, namespace: String, name: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "helm",
                "upgrade",
                "--install",
                "$name",
                "--namespace",
                namespace,
                "helm/$name"
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(redirect, command)
        }

        private fun uninstallHelmChart(redirect: Redirect?, namespace: String, name: String): Int {
            val command = listOf(
                "helm",
                "delete",
                "--purge",
                "$name"
            )
            return executeCommand(redirect, command)
        }

        private fun createResources(redirect: Redirect?, namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "create",
                "-f",
                "$path"
            )
            return executeCommand(redirect, command)
        }

        private fun replaceResources(redirect: Redirect?, namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "replace",
                "-f",
                "$path"
            )
            return executeCommand(redirect, command)
        }

        private fun deleteResources(redirect: Redirect?, namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "delete",
                "-f",
                "$path"
            )
            return executeCommand(redirect, command)
        }

        fun createNamespace(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "kubectl",
                "create",
                "namespace",
                namespace
            )
            return executeCommand(redirect, command)
        }

        fun deleteNamespace(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "namespace",
                namespace
            )
            return executeCommand(redirect, command)
        }

        private fun buildDockerImage(redirect: Redirect?, path: String, name: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "docker",
                "build",
                "-t",
                name,
                path
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(redirect, command)
        }

        private fun executeCommand(redirect: Redirect?, command: List<String>): Int {
            val processBuilder = ProcessBuilder(command)
            val environment = processBuilder.environment()
            environment["KUBECONFIG"] = System.getProperty("user.home") + "/.kube/config"
            processBuilder.redirectErrorStream(true)
            processBuilder.redirectOutput(redirect)
            val process = processBuilder.start()
            return process.waitFor()
        }
    }
}