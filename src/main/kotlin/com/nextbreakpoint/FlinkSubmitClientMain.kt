package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.float
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.command.*
import com.nextbreakpoint.model.*
import io.kubernetes.client.ApiClient
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File
import java.io.FileInputStream
import java.util.concurrent.TimeUnit

class FlinkSubmitClientMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                FlinkSubmit().subcommands(Create(), Delete(), Submit(), Cancel(), List(), Server()).main(args)
            } catch (e: Exception) {
                e.printStackTrace()
                System.exit(-1)
            }
        }

        private fun createKubernetesClient(kubeConfig: String?): ApiClient? {
            val client = if (kubeConfig != null) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
            client.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
            client.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
            client.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
            client.isDebugging = true
            return client
        }
    }

    class FlinkSubmit: CliktCommand(name = "FlinkSubmit") {
        override fun run() = Unit
    }

    class Create: CliktCommand(help="Create a cluster") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val clusterName: String by option(help="The name of the new Flink cluster").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val environment: String by option(help="The name of the environment").default("test")
        private val image: String by option(help="The Flink image to use for JobManager and TaskManager").required()
        private val imagePullPolicy: String by option(help="The image pull policy").default("IfNotPresent")
        private val imagePullSecrets: String by option(help="The image pull secrets").required()
        private val jobmanagerCpus: Float by option(help="The JobManager's cpus limit").float().default(1f)
        private val taskmanagerCpus: Float by option(help="The TaskManager's cpus limit").float().default(1f)
        private val jobmanagerMemory: Int by option(help="The JobManager's memory limit in Mb").int().default(512)
        private val taskmanageMemory: Int by option(help="The TaskManager's memory limit in Mb").int().default(1024)
        private val jobmanagerStorageSize: Int by option(help="The JobManager's storage size in Gb").int().default(2)
        private val taskmanagerStorageSize: Int by option(help="The TaskManager's storage size in Gb").int().default(5)
        private val jobmanagerStorageClass: String by option(help="The JobManager's storage class").default("standard")
        private val taskmanagerStorageClass: String by option(help="The TaskManager's storage class").default("standard")
        private val taskmanagerTaskSlots: Int by option(help="The number of task slots for each TaskManager").int().default(1)
        private val taskmanagerReplicas: Int by option(help="The number of TaskManager replicas").int().default(1)
        private val jobmanagerServiceMode: String by option(help="The JobManager's service type").default("clusterIP")

        @ExperimentalCoroutinesApi
        override fun run() {
            val config = ClusterConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jobmanager = JobManagerConfig(
                    image = image,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    serviceMode = jobmanagerServiceMode,
                    storage = StorageConfig(
                        size = jobmanagerStorageSize,
                        storageClass = jobmanagerStorageClass
                    ),
                    resources = ResourcesConfig(
                        cpus = jobmanagerCpus,
                        memory = jobmanagerMemory
                    )
                ),
                taskmanager = TaskManagerConfig(
                    image = image,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    taskSlots = taskmanagerTaskSlots,
                    replicas = taskmanagerReplicas,
                    storage = StorageConfig(
                        size = taskmanagerStorageSize,
                        storageClass = taskmanagerStorageClass
                    ),
                    resources = ResourcesConfig(
                        cpus = taskmanagerCpus,
                        memory = taskmanageMemory
                    )
                )
            )
            Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))
            CreateCluster().run(config)
        }
    }

    class Delete: CliktCommand(help="Delete a cluster") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        @ExperimentalCoroutinesApi
        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))
            DeleteCluster().run(descriptor)
        }
    }

    class Submit: CliktCommand(help="Submit a job") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val className: String by option(help="The name of the class to submit").default("")
        private val jarPath: String by option(help="The path of the jar to submit").required()
        private val arguments: String by option(help="The argument list (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").default("")
        private val fromSavepoint: String by option(help="Resume the job from the savepoint").default("")
        private val parallelism: Int by option(help="The parallelism of the job").int().default(1)

        @ExperimentalCoroutinesApi
        override fun run() {
            val config = JobSubmitConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jarPath = jarPath,
                className = if (className.isBlank()) null else className,
                arguments = if (arguments.isBlank()) null else arguments,
                savepoint = if (fromSavepoint.isBlank()) null else fromSavepoint,
                parallelism = parallelism
            )
            Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))
            SubmitJob().run(config)
        }
    }

    class Cancel: CliktCommand(help="Cancel a job") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val createSavepoint: Boolean by option(help="Create savepoint before stopping the job").flag(default = false)
        private val jobId: String by option(help="The id of the job to cancel").prompt("Insert job id")

        @ExperimentalCoroutinesApi
        override fun run() {
            val config = JobCancelConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                savepoint = createSavepoint,
                jobId = jobId
            )
            Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))
            CancelJob().run(config)
        }
    }

    class List: CliktCommand(help="List jobs") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val onlyRunning: Boolean by option(help="List only running jobs").flag(default = true)

        @ExperimentalCoroutinesApi
        override fun run() {
            val config = JobListConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                running = onlyRunning
            )
            Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))
            ListJobs().run(config)
        }
    }

    class Server: CliktCommand(help="Run Server") {
        private val port: Int by option(help="Listen on port").int().default(4444)
        private val kubeConfig: String by option(help="The path of Kubectl config").default("")

        override fun run() {
            val config = ServerConfig(
                port = port,
                kubeConfig = if (kubeConfig.isNotBlank()) kubeConfig else null
            )
            RunServer().run(config)
        }
    }
}