package com.nextbreakpoint.flink.integration

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.squareup.okhttp.MediaType
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.RequestBody
import io.kubernetes.client.openapi.JSON
import org.apache.commons.io.IOUtils
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.lang.ProcessBuilder.Redirect
import java.time.Duration
import java.util.Date
import java.util.concurrent.TimeUnit
import kotlin.test.fail

open class IntegrationSetup {
    companion object {
        val version = "1.4.4-beta"
        val flinkVersion = "1.11.3"
        val scalaVersion = "2.12"
        val timestamp = System.currentTimeMillis()
        val namespace = "integration"
        val port = getVariable("OPERATOR_PORT", "30000").toInt()
        val host = getVariable("OPERATOR_HOST", "localhost")
        val s3Endpoint = getVariable("S3_ENDPOINT", "http://minio-headless:9000")
        val s3PathStyleAccess = getVariable("S3_PATH_STYLE_ACCESS", "true")
        val s3AccessKey = getVariable("S3_ACCESS_KEY", "minioaccesskey")
        val s3SecretKey = getVariable("S3_SECRET_KEY", "miniosecretkey")
        val mapTypeToken = object : TypeToken<Map<String, Any>>() {}
        val clusterSpecTypeToken = object : TypeToken<V1FlinkClusterSpec>() {}
        val clusterStatusTypeToken = object : TypeToken<V1FlinkClusterStatus>() {}
        val jobSpecTypeToken = object : TypeToken<V1FlinkJobSpec>() {}
        val jobStatusTypeToken = object : TypeToken<V1FlinkJobStatus>() {}
        val taskmanagersTypeToken = object : TypeToken<List<TaskManagerInfo>>() {}

        private var buildDockerImages = System.getenv("BUILD_IMAGES") == "true"

        @JvmStatic
        fun setup() {
            printInfo()
            TimeUnit.SECONDS.sleep(5)
            removeFinalizers()
            deleteCRD()
            deleteNamespace()
            buildDockerImages()
            createNamespace()
            installCharts()
            installMinio()
            waitForMinio()
            createBucket()
            uninstallCRDs()
            installCRDs()
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
            uninstallCharts()
            removeFinalizers()
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
            describeDeployments(namespace = namespace)
            describeClusters(namespace = namespace)
            describeJobs(namespace = namespace)
            describePods(namespace = namespace)
        }

        fun createNamespace() {
            if (createNamespace(namespace = namespace) != 0) {
                fail("Can't create namespace")
            }
        }

        fun buildDockerImages() {
            if (!buildDockerImages) {
                return
            }
            cleanDockerImages()
            println("Building operator image...")
            if (buildDockerImage(path = ".", name = "integration/flinkctl:$version", args = emptyList()) != 0) {
                fail("Can't build operator image")
            }
            println("Building flink image...")
            val flinkBuildArgs = listOf(
                "--build-arg", "flink_version=$flinkVersion", "--build-arg", "scala_version=$scalaVersion"
            )
            if (buildDockerImage(path = "integration/flink", name = "integration/flink:$flinkVersion", args = flinkBuildArgs) != 0) {
                fail("Can't build flink image")
            }
            println("Building job image...")
            val jobBuildArgs = listOf(
                "--build-arg", "repository=integration/flinkctl", "--build-arg", "version=$version"
            )
            if (buildDockerImage(path = "integration/jobs", name = "integration/jobs:latest", args = jobBuildArgs) != 0) {
                fail("Can't build job image")
            }
            println("Images created")
            buildDockerImages = false
        }

        fun installCharts() {
            println("Installing jobs chart...")
            val args = listOf("--set=s3AccessKey=$s3AccessKey,s3SecretKey=$s3SecretKey,s3Endpoint=$s3Endpoint,s3PathStyleAccess=$s3PathStyleAccess")
            if (installHelmChart(namespace = namespace, name = "jobs", path = "integration/helm/jobs", args = args, print = false) != 0) {
                if (upgradeHelmChart(namespace = namespace, name = "jobs", path = "integration/helm/jobs", args = args, print = false) != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            println("Jobs chart installed")
        }

        fun uninstallCharts() {
            println("Uninstalling jobs chart...")
            if (uninstallHelmChart(namespace = namespace, name = "jobs") != 0) {
                println("Can't uninstall Helm chart")
            }
            println("Jobs chart uninstalled")
        }

        fun installMinio() {
            println("Installing Minio...")
            if (installHelmChart(namespace = namespace, name = "minio", path = "integration/helm/minio", args = listOf()) != 0) {
                if (upgradeHelmChart(namespace = namespace, name = "minio", path = "integration/helm/minio", args = listOf()) != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            println("Minio installed")
        }

        fun uninstallMinio() {
            println("Uninstalling Minio...")
            if (uninstallHelmChart(namespace = namespace, name = "minio") != 0) {
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
                    isMinioReady(namespace = namespace)
                }
        }

        fun createBucket() {
            println("Creating bucket...")
            if (createBucket(namespace = namespace, bucketName = "nextbreakpoint-integration") != 0) {
                fail("Can't create bucket")
            }
            println("Bucker created")
        }

        fun installCRDs() {
            println("Installing CRDs...")
            if (installHelmChart(namespace = namespace, name = "flink-k8s-toolbox-crd", path = "helm/flink-k8s-toolbox-crd") != 0) {
                if (upgradeHelmChart(namespace = namespace, name = "flink-k8s-toolbox-crd", path = "helm/flink-k8s-toolbox-crd") != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
        }
        fun uninstallCRDs() {
            println("Uninstalling CRDs...")
            if (uninstallHelmChart(namespace = namespace, name = "flink-k8s-toolbox-crd") != 0) {
                println("Can't uninstall Helm chart")
            }
            println("Operator uninstalled")
        }

        fun installOperator() {
            println("Installing operator...")
            if (installHelmChart(namespace = namespace, name = "flink-k8s-toolbox-roles", path = "helm/flink-k8s-toolbox-roles") != 0) {
                if (upgradeHelmChart(namespace = namespace, name = "flink-k8s-toolbox-roles", path = "helm/flink-k8s-toolbox-roles") != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            val args = listOf(
                "--set", "namespace=$namespace",
                "--set", "image.pullPolicy=Never",
                "--set", "image.repository=integration/flinkctl",
                "--set", "image.version=$version",
                "--set", "serviceType=NodePort",
                "--set", "serviceNodePort=$port"
            )
            if (installHelmChart(namespace = namespace, name = "flink-k8s-toolbox-operator", path = "helm/flink-k8s-toolbox-operator", args = args) != 0) {
                if (upgradeHelmChart(namespace = namespace, name = "flink-k8s-toolbox-operator", path = "helm/flink-k8s-toolbox-operator", args = args) != 0) {
                    fail("Can't install or upgrade Helm chart")
                }
            }
            println("Operator installed")
            println("Starting operator...")
            if (scaleOperator(namespace = namespace, replicas = 2) != 0) {
                fail("Can't scale the operator")
            }
            awaitUntilAsserted(timeout = 60) {
                assertThat(isOperatorRunning(namespace = namespace)).isTrue()
            }
            println("Operator started")
        }

        fun uninstallOperator() {
            println("Stopping operator...")
            if (scaleOperator(namespace = namespace, replicas = 0) != 0) {
                println("Can't scale the operator")
            }
            Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until {
                    !isOperatorRunning(namespace = namespace)
                }
            println("Operator terminated")
            println("Uninstalling operator...")
            if (uninstallHelmChart(namespace = namespace, name = "flink-k8s-toolbox-operator") != 0) {
                println("Can't uninstall Helm chart")
            }
            if (uninstallHelmChart(namespace = namespace, name = "flink-k8s-toolbox-roles") != 0) {
                println("Can't uninstall Helm chart")
            }
            println("Operator uninstalled")
        }

        fun exposeOperator() {
            println("Exposing operator...")
            if (exposeOperator(namespace = namespace) != 0) {
                fail("Can't expose the operator")
            }
            awaitUntilAsserted(timeout = 60) {
                assertThat(listClusters()).isNotEmpty()
            }
            println("Operator exposed")
        }

        fun printOperatorLogs() {
            println("Printing operator logs...")
            if (printOperatorLogs(namespace = namespace) != 0) {
                println("Can't print operator logs")
            }
            println("Operator logs printed")
        }

        fun printSupervisorLogs() {
            println("Printing supervisor logs...")
            listResources("fc").forEach {
                if (printSupervisorLogs(namespace = namespace, clusterName = it) != 0) {
                    println("Can't print supervisor logs")
                }
            }
            println("Supervisor logs printed")
        }

        fun printJobManagerLogs() {
            println("Printing JobManager logs...")
            listResources("fc").forEach {
                if (printJobManagerLogs(namespace = namespace, clusterName = it) != 0) {
                    println("Can't print JobManager logs")
                }
            }
            println("JobManager logs printed")
        }

        fun printTaskManagerLogs() {
            println("Printing TaskManager logs...")
            listResources("fc").forEach {
                if (printTaskManagerLogs(namespace = namespace, clusterName = it) != 0) {
                    println("Can't print TaskManager logs")
                }
            }
            println("TaskManager logs printed")
        }

        fun printBootstrapJobLogs() {
            println("Printing Bootstrap logs...")
            listResources("fc").forEach {
                if (printBootstrapJobLogs(namespace = namespace, clusterName = it) != 0) {
                    println("Can't print Bootstrap logs")
                }
            }
            println("Bootstrap logs printed")
        }

        fun installResources() {
            println("Install resources...")
//            if (createResources(namespace = namespace, path = "integration/sample-data.yaml") != 0) {
//                if (replaceResources(namespace = namespace, path = "integration/sample-data.yaml") != 0) {
//                    fail("Can't create data")
//                }
//            }
            println("Resources installed")
        }

        fun removeFinalizers() {
            listResources("fd").forEach {
                if (removeFinalizer(namespace = namespace, resource = "fd", name = it) != 0) {
                    println("Can't delete deployment finalizers")
                }
            }
            listResources("fc").forEach {
                if (removeFinalizer(namespace = namespace, resource = "fc", name = it) != 0) {
                    println("Can't delete cluster finalizers")
                }
            }
            listResources("fj").forEach {
                if (removeFinalizer(namespace = namespace, resource = "fj", name = it) != 0) {
                    println("Can't delete job finalizers")
                }
            }
        }

        fun listResources(resource: String): List<String> {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get $resource -o custom-columns=NAME:.metadata.name --no-headers 2> /dev/null"
            )
            val outputStream = ByteArrayOutputStream()
            if (executeCommand(command, outputStream) != 0) {
                println("Can't list resources")
                return listOf()
            }
            val output = outputStream.toString()
            return output.split("\n").filter { it.isNotBlank() }
        }

        fun deleteNamespace() {
            if (deleteNamespace(namespace = namespace) != 0) {
                println("Can't delete namespace")
            }
        }

        fun deleteCRD() {
            if (deleteCRD(name = "flinkdeployments.nextbreakpoint.com") != 0) {
                println("Can't delete CRD: flinkdeployments.nextbreakpoint.com")
            }
            if (deleteCRD(name = "flinkclusters.nextbreakpoint.com") != 0) {
                println("Can't delete CRD: flinkclusters.nextbreakpoint.com")
            }
            if (deleteCRD(name = "flinkjobs.nextbreakpoint.com") != 0) {
                println("Can't delete CRD: flinkjobs.nextbreakpoint.com")
            }
        }

        fun awaitUntilAsserted(timeout: Long = 120, delay: Long = 1, interval: Long = 5, assertion: () -> Unit) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(delay))
                .pollInterval(Duration.ofSeconds(interval))
                .untilAsserted(assertion)
        }

        fun awaitUntilCondition(timeout: Long = 120, delay: Long = 1, interval: Long = 5, condition: () -> Boolean) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(timeout))
                .pollDelay(Duration.ofSeconds(delay))
                .pollInterval(Duration.ofSeconds(interval))
                .until(condition)
        }

        private fun listClusters(): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't list clusters")
            }
            return response.second ?: mapOf()
        }

        private fun listJobs(clusterName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't list jobs")
            }
            return response.second ?: mapOf()
        }

        fun stopCluster(clusterName: String, options: StopOptions) {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/stop", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't stop cluster $clusterName")
            }
        }

        fun startCluster(clusterName: String, options: StartOptions) {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/start", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't start cluster $clusterName")
            }
        }

        fun scaleCluster(clusterName: String, options: ScaleClusterOptions) {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/scale", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't scale cluster $clusterName")
            }
        }

        fun createCluster(clusterName: String, spec: V1FlinkClusterSpec) {
            val response = sendPostRequest(url = "http://$host:$port/api/v1/clusters/$clusterName", body = spec, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't create cluster $clusterName")
            }
        }

        fun deleteCluster(clusterName: String) {
            val response = sendDeleteRequest(url = "http://$host:$port/api/v1/clusters/$clusterName", typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't delete cluster $clusterName")
            }
        }

        fun createJob(clusterName: String, jobName: String, spec: V1FlinkJobSpec) {
            val response = sendPostRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName", body = spec, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't create job $clusterName-$jobName")
            }
        }

        fun deleteJob(clusterName: String, jobName: String) {
            val response = sendDeleteRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName", typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't delete job $clusterName-$jobName")
            }
        }

        fun scaleJob(clusterName: String, jobName: String, options: ScaleJobOptions) {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/scale", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't scale job $clusterName")
            }
        }

        fun getClusterStatus(clusterName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/status", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get status of cluster $clusterName")
            }
            return response.second ?: mapOf()
        }

        fun getJobStatus(clusterName: String, jobName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/status", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get status of job $clusterName-$jobName")
            }
            return response.second ?: mapOf()
        }

        fun stopJob(clusterName: String, jobName: String, options: StopOptions) {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/stop", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't stop job $clusterName-$jobName")
            }
        }

        fun startJob(clusterName: String, jobName: String, options: StartOptions) {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/start", body = options, typeToken = mapTypeToken)
            if (response.first != 200 || response.second?.get("status") != "OK") {
                fail("Can't start job $clusterName-$jobName")
            }
        }

        fun getJobDetails(clusterName: String, jobName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/details", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get job details of cluster $clusterName-$jobName")
            }
            return response.second ?: mapOf()
        }

        fun getJobMetrics(clusterName: String, jobName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/metrics", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get job metrics of cluster $clusterName-$jobName")
            }
            return response.second ?: mapOf()
        }

        fun getJobManagerMetrics(clusterName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobmanager/metrics", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get JobManager metrics of cluster $clusterName")
            }
            return response.second ?: mapOf()
        }

        fun getTaskManagers(clusterName: String): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/taskmanagers", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get TaskManagers list of cluster $clusterName")
            }
            return response.second ?: mapOf()
        }

        fun getTaskManagerDetails(clusterName: String, taskmanagerId: TaskManagerId): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/taskmanagers/${taskmanagerId.taskmanagerId}/details", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get TaskManager details of cluster $clusterName")
            }
            return response.second ?: mapOf()
        }

        fun getTaskManagerMetrics(clusterName: String, taskmanagerId: TaskManagerId): Map<String, Any> {
            val response = sendGetRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/taskmanagers/${taskmanagerId.taskmanagerId}/metrics", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't get TaskManager metrics of cluster $clusterName")
            }
            return response.second ?: mapOf()
        }

        fun triggerSavepoint(clusterName: String, jobName: String): Map<String, Any> {
            val response = sendPutRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/savepoint", typeToken = mapTypeToken, body = "")
            if (response.first != 200) {
                fail("Can't trigger savepoint in job $clusterName-$jobName")
            }
            return response.second ?: mapOf()
        }

        fun forgetSavepoint(clusterName: String, jobName: String): Map<String, Any> {
            val response = sendDeleteRequest(url = "http://$host:$port/api/v1/clusters/$clusterName/jobs/$jobName/savepoint", typeToken = mapTypeToken)
            if (response.first != 200) {
                fail("Can't forget savepoint in job $clusterName-$jobName")
            }
            return response.second ?: mapOf()
        }

        private fun <T> sendGetRequest(timeout: Long = 60, url: String, typeToken: TypeToken<T>): Pair<Int, T?> {
            val request = Request.Builder().url(url).get().build()
            return executeCall(request, timeout, typeToken)
        }

        private fun <T> sendPostRequest(timeout: Long = 60, url: String, body: Any? = null, typeToken: TypeToken<T>): Pair<Int, T?> {
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

        private fun <T> sendPutRequest(timeout: Long = 60, url: String, body: Any? = null, typeToken: TypeToken<T>): Pair<Int, T?> {
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

        private fun <T> sendDeleteRequest(timeout: Long = 60, url: String, typeToken: TypeToken<T>): Pair<Int, T?> {
            val request = Request.Builder().url(url).delete().build()
            return executeCall(request, timeout, typeToken)
        }

        fun clusterExists(namespace: String, name: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc > /dev/null && kubectl -n $namespace get fc -o json | jq --exit-status -r '.items[] | select(.metadata.name == \"$name\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun jobExists(namespace: String, name: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fj > /dev/null && kubectl -n $namespace get fj -o json | jq --exit-status -r '.items[] | select(.metadata.name == \"$name\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasResourceStatus(namespace: String, resource: String, name: String, status: ResourceStatus): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get $resource > /dev/null && kubectl -n $namespace get $resource $name -o json | jq --exit-status -r '.status | select(.resourceStatus == \"$status\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasClusterStatus(namespace: String, name: String, status: ClusterStatus): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc > /dev/null && kubectl -n $namespace get fc $name -o json | jq --exit-status -r '.status | select(.supervisorStatus == \"$status\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasJobStatus(namespace: String, name: String, status: JobStatus): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fj > /dev/null && kubectl -n $namespace get fj $name -o json | jq --exit-status -r '.status | select(.supervisorStatus == \"$status\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasClusterJobStatus(namespace: String, name: String, status: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fj > /dev/null && kubectl -n $namespace get fj $name -o json | jq --exit-status -r '.status | select(.jobStatus == \"$status\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasTaskManagers(namespace: String, name: String, taskManagers: Int): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc > /dev/null && kubectl -n $namespace get fc $name -o json | jq --exit-status -r '.status | select(.taskManagerReplicas == $taskManagers)' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasLessOrEqualToTaskManagers(namespace: String, name: String, taskManagers: Int): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc > /dev/null && kubectl -n $namespace get fc $name -o json | jq --exit-status -r '.status | select(.taskManagerReplicas <= $taskManagers)' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun hasParallelism(namespace: String, name: String, jobParallelism: Int): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fj > /dev/null && kubectl -n $namespace get fj $name -o json | jq --exit-status -r '.status | select(.jobParallelism == $jobParallelism)' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        fun getSavepointPath(namespace: String, name: String): String? {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fj $name -o custom-columns=SAVEPOINT_PATH:.status.savepointPath --no-headers 2> /dev/null"
            )
            val outputStream = ByteArrayOutputStream()
            if (executeCommand(command, outputStream) != 0) {
                println("Can't get savepoint path")
                return null
            }
            return outputStream.toString().trim()
        }

        fun getTaskManagers(namespace: String, name: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get fc $name -o custom-columns=TASK_MANAGERS:.status.taskManagerReplicas --no-headers 2> /dev/null"
            )
            val outputStream = ByteArrayOutputStream()
            if (executeCommand(command, outputStream) != 0) {
                println("Can't get taskmanager replicas")
                return 0
            }
            return outputStream.toString().trim().toInt()
        }

        fun createResource(namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "create",
                "-f",
                path
            )
            return executeCommand(command)
        }

        fun deleteResource(namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "delete",
                "-f",
                path
            )
            return executeCommand(command)
        }

        fun describeDeployments(namespace: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "get",
                "fd"
            )
            return executeCommand(command)
        }

        fun describeClusters(namespace: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "get",
                "fc"
            )
            return executeCommand(command)
        }

        fun describeJobs(namespace: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "get",
                "fj"
            )
            return executeCommand(command)
        }

        fun describePods(namespace: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "describe",
                "pod"
            )
            return executeCommand(command)
        }

        fun updateDeployment(namespace: String, name: String, patch: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "patch",
                "fd",
                name,
                "--type",
                "json",
                "--patch",
                patch
            )
            return executeCommand(command)
        }

        fun updateCluster(namespace: String, name: String, patch: String): Int {
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
            return executeCommand(command)
        }

        fun updateJob(namespace: String, name: String, patch: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "patch",
                "fj",
                name,
                "--type",
                "json",
                "--patch",
                patch
            )
            return executeCommand(command)
        }

        private fun isOperatorRunning(namespace: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get pod --selector app=flink-operator -o json | jq --exit-status -r '.items[0].status.containerStatuses[] | select(.ready == true)' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        private fun exposeOperator(namespace: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace expose service flink-operator --type=LoadBalancer --name=flink-operator-lb --port=4444 --external-ip=\$(minikube ip)"
            )
            return executeCommand(command)
        }

        private fun printOperatorLogs(namespace: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace logs --tail=1000 -l app=flink-operator"
            )
            return executeCommand(command)
        }

        private fun printSupervisorLogs(namespace: String, clusterName: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace logs --tail=1000 -l role=supervisor,clusterName=$clusterName"
            )
            return executeCommand(command)
        }

        private fun printJobManagerLogs(namespace: String, clusterName: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace logs --tail=1000 -l role=jobmanager,clusterName=$clusterName -c jobmanager"
            )
            return executeCommand(command)
        }

        private fun printTaskManagerLogs(namespace: String, clusterName: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace logs --tail=1000 -l role=taskmanager,clusterName=$clusterName -c taskmanager"
            )
            return executeCommand(command)
        }

        private fun printBootstrapJobLogs(namespace: String, clusterName: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace logs --tail=1000 -l role=bootstrap,clusterName=$clusterName"
            )
            return executeCommand(command)
        }

        private fun saveOperatorPort(namespace: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get service -o json | jq -r '.items[0].spec.ports[] | select(.name==\"control\") | .nodePort' > .port"
            )
            return executeCommand(command)
        }

        private fun saveOperatorHost(): Int {
            val command = listOf(
                "minikube", "ip", ">", ".host"
            )
            return executeCommand(command)
        }

        private fun scaleOperator(namespace: String, replicas: Int): Int {
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
            return executeCommand(command)
        }

        private fun installHelmChart(namespace: String, name: String, path: String, args: List<String>? = emptyList(), print: Boolean = false): Int {
            val command = listOf(
                "helm",
                "install",
                "--namespace",
                namespace,
                name,
                path
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(command, print)
        }

        private fun upgradeHelmChart(namespace: String, name: String, path: String, args: List<String>? = emptyList(), print: Boolean = false): Int {
            val command = listOf(
                "helm",
                "upgrade",
                "--namespace",
                namespace,
                name,
                path
            ).plus(args?.asSequence().orEmpty())
            return executeCommand(command, print)
        }

        private fun uninstallHelmChart(namespace: String, name: String): Int {
            val command = listOf(
                "helm",
                "uninstall",
                "--namespace",
                namespace,
                name
            )
            return executeCommand(command)
        }

        fun isMinioReady(namespace: String): Boolean {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace get pod -l app=minio -o json | jq --exit-status -r '.items[0].status | select(.phase==\"Running\")' > /dev/null"
            )
            return executeCommand(command) == 0
        }

        private fun createBucket(namespace: String, bucketName: String): Int {
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
            return executeCommand(command)
        }

        private fun createResources(namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "create",
                "-f",
                "$path"
            )
            return executeCommand(command)
        }

        private fun replaceResources(namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "replace",
                "-f",
                "$path"
            )
            return executeCommand(command)
        }

        private fun deleteResources(namespace: String, path: String): Int {
            val command = listOf(
                "kubectl",
                "-n",
                namespace,
                "delete",
                "-f",
                "$path"
            )
            return executeCommand(command)
        }

        private fun createResources(path: String): Int {
            val command = listOf(
                "kubectl",
                "create",
                "-f",
                "$path"
            )
            return executeCommand(command)
        }

        private fun deleteResources(path: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "-f",
                "$path"
            )
            return executeCommand(command)
        }

        private fun createNamespace(namespace: String): Int {
            val command = listOf(
                "kubectl",
                "create",
                "namespace",
                namespace
            )
            return executeCommand(command)
        }

        private fun deleteNamespace(namespace: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "namespace",
                namespace
            )
            return executeCommand(command)
        }

        private fun deleteCRD(name: String): Int {
            val command = listOf(
                "kubectl",
                "delete",
                "crd",
                name
            )
            return executeCommand(command)
        }

        private fun buildDockerImage(path: String, name: String, args: List<String>? = emptyList()): Int {
            val command = listOf(
                "sh",
                "-c",
                "eval $(minikube docker-env) && docker build -t $name $path ${args?.asSequence().orEmpty().joinToString(" ")}"
            )
            return executeCommand(command)
        }

        private fun cleanDockerImages(): Int {
            val command = listOf(
                "sh",
                "-c",
                "eval $(minikube docker-env) && docker rmi $(docker images -f dangling=true -q)"
            )
            return executeCommand(command)
        }

        private fun removeFinalizer(namespace: String, resource: String, name: String): Int {
            val command = listOf(
                "sh",
                "-c",
                "kubectl -n $namespace patch $resource $name --type=json -p '[{\"op\":\"replace\",\"path\":\"/metadata/finalizers\",\"value\":[]}]'"
            )
            return executeCommand(command)
        }

        private fun createApiClient(timeout: Long): OkHttpClient {
            val client = OkHttpClient()
            client.setReadTimeout(timeout, TimeUnit.SECONDS)
            client.setWriteTimeout(timeout, TimeUnit.SECONDS)
            return client
        }

        private fun <T> executeCall(request: Request, timeout: Long, typeToken: TypeToken<T>): Pair<Int, T?> {
            try {
                val response = createApiClient(timeout).newCall(request).execute()
                response.body().use {
                    val line = it.source().readUtf8Line()
                    println("Response code: ${response.code()}, body: $line")
                    return response.code() to JSON().deserialize(line, typeToken.type)
                }
            } catch (e: Exception) {
                println("ERROR: ${e.message}")
                return 500 to null
            }
        }

        private fun executeCommand(command: List<String>, print: Boolean = true): Int {
            if (print) println(command.joinToString(prefix = "# ", separator = " "))
            val processBuilder = ProcessBuilder(command)
            val environment = processBuilder.environment()
            environment["KUBECONFIG"] = System.getenv("KUBECONFIG") ?: System.getProperty("user.home") + "/.kube/config"
            processBuilder.redirectErrorStream(true)
            processBuilder.redirectOutput(Redirect.INHERIT)
            val process = processBuilder.start()
            return process.waitFor()
        }

        private fun executeCommand(command: List<String>, outputStream: OutputStream, print: Boolean = true): Int {
            if (print) println(command.joinToString(prefix = "# ", separator = " "))
            val processBuilder = ProcessBuilder(command)
            val environment = processBuilder.environment()
            environment["KUBECONFIG"] = System.getenv("KUBECONFIG") ?: System.getProperty("user.home") + "/.kube/config"
            processBuilder.redirectErrorStream(true)
            processBuilder.redirectOutput(Redirect.PIPE)
            val process = processBuilder.start()
            val waitFor = process.waitFor()
            IOUtils.copy(process.inputStream, outputStream)
            return waitFor
        }

        private fun getVariable(name: String, defaultValue: String): String {
            return System.getenv(name) ?: defaultValue
        }

        private fun getVariable(name: String): String {
            return System.getenv(name) ?: throw IllegalStateException("Missing required environment variable: $name")
        }
    }
}
