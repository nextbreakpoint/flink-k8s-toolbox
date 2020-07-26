package com.nextbreakpoint.flinkoperator.integration

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.squareup.okhttp.MediaType
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.RequestBody
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import java.lang.ProcessBuilder.Redirect
import java.time.Duration
import java.util.Date
import java.util.concurrent.TimeUnit
import kotlin.test.fail

open class IntegrationSetup {
    companion object {
        val redirect = Redirect.INHERIT
        val version = "1.3.4-beta"
        val timestamp = System.currentTimeMillis()
//        val namespace = "integration-$timestamp"
        val namespace = "integration"
        val port = getVariable("OPERATOR_PORT", "30000").toInt()
        val host = getVariable("OPERATOR_HOST", "localhost")
        val mapTypeToken = object : TypeToken<Map<String, Any>>() {}
        val listTypeToken = object : TypeToken<List<String>>() {}
        val specTypeToken = object : TypeToken<V1FlinkClusterSpec>() {}
        val statusTypeToken = object : TypeToken<V1FlinkClusterStatus>() {}
        val taskmanagersTypeToken = object : TypeToken<List<TaskManagerInfo>>() {}

        private var buildDockerImages = System.getenv("BUILD_IMAGES") == "true"

        @JvmStatic
        fun setup() {
            buildDockerImages()
            TimeUnit.SECONDS.sleep(5)
            printInfo()
            deleteCRD()
            deleteNamespace()
            createNamespace()
            installMinio()
            waitForMinio()
            createBucket()
            installOperator()
            installResources()
            exposeOperator()
            TimeUnit.SECONDS.sleep(5)
        }

        @JvmStatic
        fun teardown() {
            TimeUnit.SECONDS.sleep(5)
            uninstallOperator()
            uninstallMinio()
            TimeUnit.SECONDS.sleep(5)
            deleteNamespace()
        }

        fun printInfo() {
            println("Run test - ${Date(timestamp)}")
            println("Namespace = $namespace")
            println("Version = $version")
            println("Build images = ${if (buildDockerImages) "Yes" else "No"}")
        }

        fun describeResources() {
            describePods(redirect = redirect, namespace = namespace)
            describeClusters(redirect = redirect, namespace = namespace)
        }

        fun createNamespace() {
            if (createNamespace(redirect = redirect, namespace = namespace) != 0) {
                fail("Can't create namespace")
            }
        }

        fun cleanDockerImages() {
            cleanDockerImages(redirect = redirect)
        }

        fun buildDockerImages() {
            if (!buildDockerImages) {
                return
            }
            cleanDockerImages()
            println("Building operator image...")
            if (buildDockerImage(redirect = redirect, path = ".", name = "integration/flink-k8s-toolbox:$version", args = emptyList()) != 0) {
                fail("Can't build operator image")
            }
            println("Building flink image...")
            val flinkBuildArgs = listOf(
                "--build-arg", "flink_version=1.9.2", "--build-arg", "scala_version=2.11"
            )
            if (buildDockerImage(redirect = redirect, path = "example/flink", name = "integration/flink:1.9.2", args = flinkBuildArgs) != 0) {
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
            buildDockerImages = false
        }

        fun installMinio() {
            println("Installing Minio...")
            if (installHelmChart(redirect = redirect, namespace = namespace, name = "minio", path = "integration/minio", args = listOf()) != 0) {
                if (upgradeHelmChart(redirect = redirect, namespace = namespace, name = "minio", path = "integration/minio", args = listOf()) != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            println("Minio installed")
        }

        fun uninstallMinio() {
            println("Uninstalling Minio...")
            if (uninstallHelmChart(redirect = redirect, namespace = namespace, name = "minio") != 0) {
                println("Can't uninstall Helm chart")
            }
            println("Minio uninstalled")
        }

        fun waitForMinio() {
            Awaitility.await()
                .atMost(Duration.ofSeconds(120))
                .pollDelay(Duration.ofSeconds(20))
                .pollInterval(Duration.ofSeconds(10))
                .until {
                    isMinioReady(redirect = redirect, namespace = namespace)
                }
        }

        fun createBucket() {
            println("Creating bucket...")
            if (createBucket(redirect = redirect, namespace = namespace, bucketName = "flink") != 0) {
                fail("Can't create bucket")
            }
            println("Bucker created")
        }

        fun removeFinalizers(name: String) {
            removeFinalizers(redirect = redirect, namespace = namespace, name = name)
        }

        fun installOperator() {
            println("Installing operator...")
            if (installHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-crd", path = "helm/flink-k8s-toolbox-crd") != 0) {
                if (upgradeHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-crd", path = "helm/flink-k8s-toolbox-crd") != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            if (installHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-roles", path = "helm/flink-k8s-toolbox-roles") != 0) {
                if (upgradeHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-roles", path = "helm/flink-k8s-toolbox-roles") != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            val args = listOf(
                "--set", "namespace=$namespace",
                "--set", "image.pullPolicy=Never",
                "--set", "image.repository=integration/flink-k8s-toolbox",
                "--set", "image.version=$version",
                "--set", "serviceType=NodePort",
                "--set", "serviceNodePort=$port"
            )
            if (installHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-operator", path = "helm/flink-k8s-toolbox-operator", args = args) != 0) {
                if (upgradeHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-operator", path = "helm/flink-k8s-toolbox-operator", args = args) != 0) {
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

        fun uninstallOperator() {
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
            if (uninstallHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-roles") != 0) {
                println("Can't uninstall Helm chart")
            }
            if (uninstallHelmChart(redirect = redirect, namespace = namespace, name = "flink-k8s-toolbox-crd") != 0) {
                println("Can't uninstall Helm chart")
            }
            println("Operator uninstalled")
        }

        fun exposeOperator() {
            println("Exposing operator...")
            if (exposeOperator(redirect = redirect, namespace = namespace) != 0) {
                fail("Can't expose the operator")
            }
            awaitUntilAsserted(timeout = 60) {
                assertThat(listClusters(port = port)).isEmpty()
            }
            println("Operator exposed")
        }

        fun printOperatorLogs() {
            println("Printing operator logs...")
            if (printOperatorLogs(redirect = redirect, namespace = namespace) != 0) {
                fail("Can't expose the operator")
            }
            println("Operator logs printed")
        }

        fun installResources() {
            println("Install resources...")
            if (createResources(redirect = redirect, namespace = namespace, path = "example/config.yaml") != 0) {
                if (replaceResources(redirect = redirect, namespace = namespace, path = "example/config.yaml") != 0) {
                    fail("Can't create configmap")
                }
            }
            if (createResources(redirect = redirect, namespace = namespace, path = "example/secrets.yaml") != 0) {
                if (replaceResources(redirect = redirect, namespace = namespace, path = "example/secrets.yaml") != 0) {
                    fail("Can't create secrets")
                }
            }
            if (createResources(redirect = redirect, namespace = namespace, path = "example/data.yaml") != 0) {
                if (replaceResources(redirect = redirect, namespace = namespace, path = "example/data.yaml") != 0) {
                    fail("Can't create data")
                }
            }
            println("Resources installed")
        }

        fun deleteNamespace() {
            if (deleteNamespace(redirect = redirect, namespace = namespace) != 0) {
                println("Can't delete namespace")
            }
        }

        fun deleteCRD() {
            if (deleteCRD(redirect = redirect, name = "flinkclusters.nextbreakpoint.com") != 0) {
                println("Can't delete CRD")
            }
        }

        fun awaitUntilAsserted(timeout: Long, assertion: () -> Unit) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(10))
                .pollInterval(Duration.ofSeconds(10))
                .untilAsserted(assertion)
        }

        fun awaitUntilCondition(timeout: Long, condition: () -> Boolean) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(10))
                .pollInterval(Duration.ofSeconds(10))
                .until(condition)
        }

        fun awaitUntilAsserted(timeout: Long, delay: Long, interval: Long, assertion: () -> Unit) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(delay))
                .pollInterval(Duration.ofSeconds(interval))
                .untilAsserted(assertion)
        }

        fun awaitUntilCondition(timeout: Long, delay: Long, interval: Long, condition: () -> Boolean) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(delay))
                .pollInterval(Duration.ofSeconds(interval))
                .until(condition)
        }

        private fun listClusters(port: Int): List<String> {
            val response = sendGetRequest(url = "http://$host:$port/clusters", typeToken = listTypeToken)
            if (response.first != 200) {
                fail("Can't list clusters")
            }
            return response.second
        }

        fun stopCluster(name: String, options: StopOptions, port: Int) {
            val response = sendPutRequest(url = "http://$host:$port/cluster/$name/stop", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second["status"] != "OK") {
                fail("Can't stop cluster $name")
            }
        }

        fun startCluster(name: String, options: StartOptions, port: Int) {
            val response = sendPutRequest(url = "http://$host:$port/cluster/$name/start", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second["status"] != "OK") {
                fail("Can't start cluster $name")
            }
        }

        fun scaleCluster(name: String, options: ScaleOptions, port: Int) {
            val response = sendPutRequest(url = "http://$host:$port/cluster/$name/scale", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second["status"] != "OK") {
                fail("Can't scale cluster $name")
            }
        }

        fun createCluster(name: String, spec: V1FlinkClusterSpec, port: Int) {
            val response = sendPostRequest(url = "http://$host:$port/cluster/$name", body = spec, typeToken = mapTypeToken)
            if (response.first != 200 || response.second["status"] != "OK") {
                fail("Can't create cluster $name")
            }
        }

        fun deleteCluster(name: String, port: Int) {
            val response = sendDeleteRequest(url = "http://$host:$port/cluster/$name", typeToken = mapTypeToken)
            if (response.first != 200 || response.second["status"] != "OK") {
                fail("Can't delete cluster $name")
            }
        }

        fun getClusterStatus(name: String, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/status", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get status of cluster $name")
            }
            return response.second
        }

        fun getJobDetails(name: String, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/job/details", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get job details of cluster $name")
            }
            return response.second
        }

        fun getJobMetrics(name: String, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/job/metrics", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get job metrics of cluster $name")
            }
            return response.second
        }

        fun getJobManagerMetrics(name: String, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/jobmanager/metrics", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get JobManager metrics of cluster $name")
            }
            return response.second
        }

        fun getTaskManagers(name: String, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/taskmanagers", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get TaskManagers list of cluster $name")
            }
            return response.second
        }

        fun getTaskManagerDetails(name: String, taskmanagerId: TaskManagerId, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/taskmanagers/${taskmanagerId.taskmanagerId}/details", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get TaskManager details of cluster $name")
            }
            return response.second
        }

        fun getTaskManagerMetrics(name: String, taskmanagerId: TaskManagerId, port: Int): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/cluster/$name/taskmanagers/${taskmanagerId.taskmanagerId}/metrics", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get TaskManager metrics of cluster $name")
            }
            return response.second
        }

        fun triggerSavepoint(name: String, port: Int): Map<String, Any> {
            val response = sendPutRequest(url = "http://$host:$port/cluster/$name/savepoint", typeToken = mapTypeToken, body = "")
            if (response.first != 200) {
                fail("Can't trigger savepoint in cluster $name")
            }
            return response.second
        }

        private fun <T> sendGetRequest(timeout: Long = 60, url: String, typeToken: TypeToken<T>): Pair<Int, T> {
            val request = Request.Builder().url(url).get().build()
            return executeCall(request, timeout, typeToken)
        }

        private fun <T> sendPostRequest(timeout: Long = 60, url: String, body: Any? = null, typeToken: TypeToken<T>): Pair<Int, T> {
            val mediaType = MediaType.parse("application/json")
            val builder = Request.Builder().url(url)
            if (body != null) {
                val jsonBody = JSON().serialize(body)
                val payload = RequestBody.create(mediaType, jsonBody)
                builder.post(payload)
            }
            val request = builder.build()
            return executeCall(request, timeout, typeToken)
        }

        private fun <T> sendPutRequest(timeout: Long = 60, url: String, body: Any? = null, typeToken: TypeToken<T>): Pair<Int, T> {
            val mediaType = MediaType.parse("application/json")
            val builder = Request.Builder().url(url)
            if (body != null) {
                val jsonBody = JSON().serialize(body)
                val payload = RequestBody.create(mediaType, jsonBody)
                builder.put(payload)
            }
            val request = builder.build()
            return executeCall(request, timeout, typeToken)
        }

        private fun <T> sendDeleteRequest(timeout: Long = 60, url: String, typeToken: TypeToken<T>): Pair<Int, T> {
            val request = Request.Builder().url(url).delete().build()
            return executeCall(request, timeout, typeToken)
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

        fun hasActiveTaskManagers(redirect: Redirect?, namespace: String, name: String, taskManagers: Int): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc $name -o json | jq --exit-status -r '.status | select(.activeTaskManagers == $taskManagers)' >/dev/null"
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

        fun deleteClusterByName(redirect: Redirect?, namespace: String, name: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "delete",
                name
            )
            return executeCommand(redirect, command)
        }

        fun describeClusters(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "get",
                "fc"
            )
            return executeCommand(redirect, command)
        }

        fun describePods(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "describe",
                "pod"
            )
            return executeCommand(redirect, command)
        }

        fun updateCluster(redirect: Redirect?, namespace: String, name: String, patch: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "patch",
                "fc",
                name,
                "--type",
                "json",
                "--patch",
                patch
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

        private fun exposeOperator(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace expose service flink-operator --type=LoadBalancer --name=flink-operator-lb --port=4444 --external-ip=\$(minikube ip)"
            )
            return executeCommand(redirect, command)
        }

        private fun printOperatorLogs(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace logs --tail=1000 -l app=flink-operator"
            )
            return executeCommand(redirect, command)
        }

        private fun saveOperatorPort(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get service -o json | jq -r '.items[0].spec.ports[] | select(.name==\"control\") | .nodePort' > .port"
            )
            return executeCommand(redirect, command)
        }

        private fun saveOperatorHost(redirect: Redirect?): Int {
            val command = listOf(
                "minikube", "ip", ">", ".host"
            )
            return executeCommand(redirect, command)
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

        private fun installHelmChart(redirect: Redirect?, namespace: String, name: String, path: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "helm",
                "install",
                "--namespace",
                namespace,
                name,
                path
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(redirect, command)
        }

        private fun upgradeHelmChart(redirect: Redirect?, namespace: String, name: String, path: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "helm",
                "upgrade",
                "--namespace",
                namespace,
                name,
                path
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(redirect, command)
        }

        private fun uninstallHelmChart(redirect: Redirect?, namespace: String, name: String): Int {
            val command = listOf(
                "helm",
                "uninstall",
                "--namespace",
                namespace,
                name
            )
            return executeCommand(redirect, command)
        }

        fun isMinioReady(redirect: Redirect?, namespace: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get pod -l app=minio -o json | jq --exit-status -r '.items[0].status | select(.phase==\"Running\")' >/dev/null"
            )
            return executeCommand(redirect, command) == 0
        }

        private fun createBucket(redirect: Redirect?, namespace: String, bucketName: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "run",
                "minio-client",
                "--image=minio/mc:latest",
                "--restart=Never",
                "--command=true",
                "--",
                "sh",
                "-c",
                "mc config host add minio http://minio-headless:9000 minioaccesskey miniosecretkey && mc mb --region=eu-west-1 minio/$bucketName"
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

        private fun createResources(redirect: Redirect?, path: String): Int {
            val command = listOf(
                "kubectl",
                "create",
                "-f",
                "$path"
            )
            return executeCommand(redirect, command)
        }

        private fun deleteResources(redirect: Redirect?, path: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "-f",
                "$path"
            )
            return executeCommand(redirect, command)
        }

        private fun createNamespace(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "kubectl",
                "create",
                "namespace",
                namespace
            )
            return executeCommand(redirect, command)
        }

        private fun deleteNamespace(redirect: Redirect?, namespace: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "namespace",
                namespace
            )
            return executeCommand(redirect, command)
        }

        private fun deleteCRD(redirect: Redirect?, name: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "crd",
                name
            )
            return executeCommand(redirect, command)
        }

        private fun buildDockerImage(redirect: Redirect?, path: String, name: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "sh",
                "-c",
                "eval $(minikube docker-env) && docker build -t $name $path ${args?.asSequence().orEmpty().joinToString(" ")}"
            )
            return executeCommand(redirect, command)
        }

        private fun cleanDockerImages(redirect: Redirect?): Int {
            val command = listOf(
                "sh",
                "-c",
                "eval $(minikube docker-env) && docker rmi $(docker images -f dangling=true -q)"
            )
            return executeCommand(redirect, command)
        }

        private fun removeFinalizers(redirect: Redirect?, namespace: String, name: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace patch fc $name --type=json -p '[{\"op\":\"remove\",\"path\":\"/metadata/finalizers\"}]'"
            )
            return executeCommand(redirect, command)
        }

        private fun createApiClient(timeout: Long): OkHttpClient {
            val client = OkHttpClient()
            client.setReadTimeout(timeout, TimeUnit.SECONDS)
            client.setWriteTimeout(timeout, TimeUnit.SECONDS)
            return client
        }

        private fun <T> executeCall(request: Request, timeout: Long, typeToken: TypeToken<T>): Pair<Int, T> {
            val response = createApiClient(timeout).newCall(request).execute()
            response.body().use {
                val line = it.source().readUtf8Line()
                println("Response { code: ${response.code()}, body: $line }")
                return response.code() to JSON().deserialize(line, typeToken.type)
            }
        }

        private fun executeCommand(redirect: Redirect?, command: List<String>): Int {
            val processBuilder = ProcessBuilder(command)
            val environment = processBuilder.environment()
            environment["KUBECONFIG"] = System.getenv("KUBECONFIG") ?: System.getProperty("user.home") + "/.kube/config"
            processBuilder.redirectErrorStream(true)
            processBuilder.redirectOutput(redirect)
            val process = processBuilder.start()
            return process.waitFor()
        }

        private fun getVariable(name: String, defaultValue: String): String {
            return System.getenv(name) ?: defaultValue
        }
    }
}
