package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.float
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.command.*
import com.nextbreakpoint.model.*
import kotlinx.coroutines.ExperimentalCoroutinesApi

class FlinkSubmitMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                FlinkSubmit().subcommands(Create(), Delete(), Submit(), Cancel(), List()).main(args)
                System.exit(0)
            } catch (e: Exception) {
                e.printStackTrace()
                System.exit(-1)
            }
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
            CreateCluster().run(kubeConfig, config)
        }
    }

    class Delete: CliktCommand(help="Delete a cluster") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            DeleteCluster().run(kubeConfig, descriptor)
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
            SubmitJob().run(kubeConfig, config)
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
            CancelJob().run(kubeConfig, config)
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
            ListJobs().run(kubeConfig, config)
        }
    }
}