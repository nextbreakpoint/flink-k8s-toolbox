package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.float
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.command.*
import com.nextbreakpoint.model.*
import io.kubernetes.client.Configuration

class FlinkK8OPSMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                MainCommand().subcommands(
                    Controller().subcommands(
                        RunControllerCommand()
                    ),
                    Operator().subcommands(
                        RunOperatorCommand()
                    ),
                    Cluster().subcommands(
                        CreateClusterCommand(),
                        DeleteClusterCommand()
                    ),
                    Sidecar().subcommands(
                        SidecarSubmitCommand(),
                        SidecarWatchCommand()
                    ),
                    Job().subcommands(
                        RunJobCommand(),
                        CancelJobCommand(),
                        GetJobDetailsCommand(),
                        GetJobMetricsCommand()
                    ),
                    Jobs().subcommands(
                        ListJobsCommand()
                    ),
                    JobManager().subcommands(
                        GetJobManagerMetricsCommand()
                    ),
                    TaskManager().subcommands(
                        GetTaskManagerDetailsCommand(),
                        GetTaskManagerMetricsCommand()
                    ),
                    TaskManagers().subcommands(
                        ListTaskManagersCommand()
                    )
                ).main(args)
                System.exit(0)
            } catch (e: Exception) {
                System.exit(-1)
            }
        }
    }

    class MainCommand: CliktCommand(name = "flink-k8-ops") {
        override fun run() = Unit
    }

    class Controller: CliktCommand(name = "controller", help = "Access controller subcommands") {
        override fun run() = Unit
    }

    class Operator: CliktCommand(name = "operator", help = "Access operator subcommands") {
        override fun run() = Unit
    }

    class Cluster: CliktCommand(name = "cluster", help = "Access cluster subcommands") {
        override fun run() = Unit
    }

    class Sidecar: CliktCommand(name = "sidecar", help = "Access sidecar subcommands") {
        override fun run() = Unit
    }

    class Job: CliktCommand(name = "job", help = "Access job subcommands") {
        override fun run() = Unit
    }

    class Jobs: CliktCommand(name = "jobs", help = "Access jobs subcommands") {
        override fun run() = Unit
    }

    class JobManager: CliktCommand(name = "jobmanager", help = "Access JobManager subcommands") {
        override fun run() = Unit
    }

    class TaskManager: CliktCommand(name = "taskmanager", help = "Access TaskManager subcommands") {
        override fun run() = Unit
    }

    class TaskManagers: CliktCommand(name = "taskmanagers", help = "Access TaskManagers subcommands") {
        override fun run() = Unit
    }

    class CreateClusterCommand: CliktCommand(name = "create", help="Create a cluster") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val clusterName: String by option(help="The name of the new Flink cluster").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val environment: String by option(help="The name of the environment").default("test")
        private val flinkImage: String by option(help="The image to use for JobManager and TaskManager").required()
        private val sidecarImage: String by option(help="The image to use for Flink Submit sidecar").required()
        private val sidecarArgument: List<String> by option(help="The argument for Flink Submit sidecar").multiple()
        private val sidecarArguments: String by option(help="The arguments for Flink Submit sidecar").default("")
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
                    image = flinkImage,
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
                    image = flinkImage,
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
                ),
                sidecar = SidecarConfig(
                    image = sidecarImage,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    arguments = if (sidecarArguments.isNotBlank()) sidecarArguments else sidecarArgument.joinToString(" ")
                )
            )
            PostClusterCreateRequest().run(ApiParams(host, port), config)
        }
    }

    class DeleteClusterCommand: CliktCommand(name = "delete", help="Delete a cluster") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            PostClusterDeleteRequest().run(ApiParams(host, port), descriptor)
        }
    }

    class RunJobCommand: CliktCommand(name="run", help="Run a job") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val sidecarImage: String by option(help="The image to use for Flink Submit sidecar").required()
        private val sidecarArgument: List<String> by option(help="The argument for Flink Submit sidecar").multiple()
        private val sidecarArguments: String by option(help="The arguments for Flink Submit sidecar").default("")
        private val imagePullPolicy: String by option(help="The image pull policy").default("IfNotPresent")
        private val imagePullSecrets: String by option(help="The image pull secrets").required()

        override fun run() {
            val config = JobRunParams(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                sidecar = SidecarConfig(
                    image = sidecarImage,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    arguments = if (sidecarArguments.isNotBlank()) sidecarArguments else sidecarArgument.joinToString(" ")
                )
            )
            PostJobRunRequest().run(ApiParams(host, port), config)
            System.exit(0)
        }
    }

    class ListJobsCommand: CliktCommand(name="list", help="List jobs") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val onlyRunning: Boolean by option(help="List only running jobs").flag(default = true)

        override fun run() {
            val config = JobsListParams(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                running = onlyRunning
            )
            PostJobsListRequest().run(ApiParams(host, port), config)
        }
    }

    class CancelJobCommand: CliktCommand(name = "cancel", help="Cancel a job") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val createSavepoint: Boolean by option(help="Create savepoint before stopping the job").flag(default = false)
        private val savepointPath: String by option(help="Directory where to save savepoint").default("file:///var/tmp/savepoints")
        private val jobId: String by option(help="The id of the job to cancel").prompt("Insert job id")

        override fun run() {
            val config = JobCancelParams(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                savepoint = createSavepoint,
                savepointPath = savepointPath,
                jobId = jobId
            )
            PostJobCancelRequest().run(ApiParams(host, port), config)
        }
    }

    class GetJobDetailsCommand: CliktCommand(name = "details", help="Get job's details") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val jobId: String by option(help="The id of the job").prompt("Insert job id")

        override fun run() {
            val config = JobDescriptor(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jobId = jobId
            )
            PostJobDetailsRequest().run(ApiParams(host, port), config)
        }
    }

    class GetJobMetricsCommand: CliktCommand(name = "metrics", help="Get job's metrics") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val jobId: String by option(help="The id of the job").prompt("Insert job id")

        override fun run() {
            val config = JobDescriptor(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jobId = jobId
            )
            PostJobMetricsRequest().run(ApiParams(host, port), config)
        }
    }

    class GetJobManagerMetricsCommand: CliktCommand(name = "metrics", help="Get JobManager's metrics") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            PostJobManagerMetricsRequest().run(ApiParams(host, port), descriptor)
        }
    }

    class GetTaskManagerDetailsCommand: CliktCommand(name = "details", help="Get TaskManager's details") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val taskmanagerId: String by option(help="The id of the TaskManager").prompt("Insert TaskManager id")

        override fun run() {
            val config = TaskManagerDescriptor(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                taskmanagerId = taskmanagerId
            )
            PostTaskManagerDetailsRequest().run(ApiParams(host, port), config)
        }
    }

    class GetTaskManagerMetricsCommand: CliktCommand(name = "metrics", help="Get TaskManager's metrics") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val taskmanagerId: String by option(help="The id of the TaskManager").prompt("Insert TaskManager id")

        override fun run() {
            val config = TaskManagerDescriptor(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                taskmanagerId = taskmanagerId
            )
            PostTaskManagerMetricsRequest().run(ApiParams(host, port), config)
        }
    }

    class ListTaskManagersCommand: CliktCommand(name="list", help="List TaskManagers") {
        private val host: String by option(help="The controller address").default("localhost")
        private val port: Int by option(help="The controller port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            PostTaskManagersListRequest().run(ApiParams(host, port), descriptor)
        }
    }

    class RunControllerCommand: CliktCommand(name = "run", help="Run the controller") {
        private val port: Int by option(help="Listen on port").int().default(4444)
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of Kubectl config")

        override fun run() {
            val config = ControllerConfig(
                port = port,
                portForward = portForward,
                kubeConfig = kubeConfig
            )
            RunController().run(config)
        }
    }

    class RunOperatorCommand: CliktCommand(name="run", help="Run the operator") {
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val kubeConfig: String? by option(help="The path of kuke config")

        override fun run() {
            Configuration.setDefaultApiClient(CommandUtils.createKubernetesClient(kubeConfig))
            RunOperator().run(OperatorConfig(namespace))
        }
    }

    class SidecarSubmitCommand: CliktCommand(name="submit", help="Submit a job and monitor cluster jobs") {
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of kuke config")
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val className: String? by option(help="The name of the class to submit")
        private val jarPath: String by option(help="The path of the jar to submit").required()
        private val arguments: String by option(help="The job's arguments (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").default("")
        private val argument: List<String> by option(help="The job's argument (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").multiple()
        private val fromSavepoint: String? by option(help="Resume the job from the savepoint")
        private val parallelism: Int by option(help="The parallelism of the job").int().default(1)

        override fun run() {
            val config = JobSubmitParams(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jarPath = jarPath,
                className = className,
                arguments = if (arguments.isNotBlank()) arguments else argument.joinToString(" "),
                savepoint = fromSavepoint,
                parallelism = parallelism
            )
            Configuration.setDefaultApiClient(CommandUtils.createKubernetesClient(kubeConfig))
            RunSidecarSubmit().run(portForward, kubeConfig != null, config)
        }
    }

    class SidecarWatchCommand: CliktCommand(name="watch", help="Monitor cluster jobs") {
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of kuke config")
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val config = WatchParams(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                )
            )
            Configuration.setDefaultApiClient(CommandUtils.createKubernetesClient(kubeConfig))
            RunSidecarWatch().run(portForward, kubeConfig != null, config)
        }
    }
}