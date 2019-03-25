package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.float
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.command.*
import com.nextbreakpoint.model.*

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

    class FlinkSubmit: CliktCommand() {
        override fun run() = Unit
    }

    class Create: CliktCommand(help="Create a cluster") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val dockerImage: String by option(help="The name of Docker image with Flink job").required()
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val environment: String by option(help="The name of the environment").default("test")
        private val storageClass: String by option(help="The name of the storage class").default("standard")
        private val pullPolicy: String by option(help="The image pull policy").default("IfNotPresent")
        private val pullSecrets: String by option(help="The image pull secrets").required()
        private val jobmanagerLimitCpus: Float by option(help="The JobManager's cpus limit").float().default(1f)
        private val taskmanagerLimitCpus: Float by option(help="The TaskManager's cpus limit").float().default(1f)
        private val jobmanagerLimitMemory: Int by option(help="The JobManager's memory limit in Mb").int().default(512)
        private val taskmanagerLimitMemory: Int by option(help="The TaskManager's memory limit in Mb").int().default(1024)
        private val jobmanagerStorage: Int by option(help="The JobManager's storage size in Gb").int().default(2)
        private val taskmanagerStorage: Int by option(help="The TaskManager's storage size in Gb").int().default(5)
        private val taskmanagerTaskSlots: Int by option(help="The number of task slots for each TaskManager").int().default(1)
        private val taskmanagerReplicas: Int by option(help="The number of TaskManager replicas").int().default(1)

        override fun run() {
            val config = ClusterConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jobmanager = JobManagerConfig(
                    image = dockerImage,
                    pullPolicy = pullPolicy,
                    pullSecrets = pullSecrets,
                    storage = StorageConfig(
                        size = jobmanagerStorage,
                        storageClass = storageClass
                    ),
                    resources = ResourcesConfig(
                        cpus = jobmanagerLimitCpus,
                        memory = jobmanagerLimitMemory
                    )
                ),
                taskmanager = TaskManagerConfig(
                    image = dockerImage,
                    pullPolicy = pullPolicy,
                    pullSecrets = pullSecrets,
                    taskSlots = taskmanagerTaskSlots,
                    replicas = taskmanagerReplicas,
                    storage = StorageConfig(
                        size = taskmanagerStorage,
                        storageClass = storageClass
                    ),
                    resources = ResourcesConfig(
                        cpus = taskmanagerLimitCpus,
                        memory = taskmanagerLimitMemory
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
        private val className: String by option(help="The name of the class to submit").required()
        private val jarPath: String by option(help="The path of the jar to submit").required()
        private val savepoint: String by option(help="The path of the jar to submit").default("")
        private val arguments: String by option(help="The arguments to submit").default("")

        override fun run() {
            val config = JobSubmitConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                className = className,
                jarPath = jarPath,
                arguments = expandArguments(arguments),
                savepoint = if (savepoint.length > 0) savepoint else null
            )
            SubmitJob().run(kubeConfig, config)
        }

        private fun expandArguments(arguments: String) =
            arguments.split(",").map { it.split("=") }.map { Pair(it[0], it[1]) }.toList()

    }

    class Cancel: CliktCommand(help="Cancel a job") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val jobId: String by option(help="The id of the job to cancel").prompt("Insert job id")

        override fun run() {
            val config = JobCancelConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                savepoint = false,
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

        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            ListJobs().run(kubeConfig, descriptor)
        }
    }
}