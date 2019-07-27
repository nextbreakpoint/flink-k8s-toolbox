package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.common.CommandFactory
import com.nextbreakpoint.common.DefaultCommandFactory
import com.nextbreakpoint.common.FlinkClusterSpecification
import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.Address
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.ScaleOptions
import com.nextbreakpoint.common.model.StartOptions
import com.nextbreakpoint.common.model.StopOptions
import com.nextbreakpoint.common.model.TaskManagerId
import com.nextbreakpoint.common.model.UploadOptions
import com.nextbreakpoint.operator.OperatorConfig
import org.apache.log4j.Logger
import java.io.File
import java.nio.file.Files

class FlinkK8SToolboxMain(private val factory: CommandFactory) {
    companion object {
        private val logger = Logger.getLogger(FlinkK8SToolboxMain::class.simpleName)

        @JvmStatic
        fun main(args: Array<String>) {
            FlinkK8SToolboxMain(DefaultCommandFactory).run(args)
        }
    }

    fun run(args: Array<String>) {
        try {
            MainCommand().subcommands(
                Operator().subcommands(
                    RunOperatorCommand(factory)
                ),
                Cluster().subcommands(
                    StartClusterCommand(factory),
                    StopClusterCommand(factory),
                    CreateClusterCommand(factory),
                    DeleteClusterCommand(factory)
                ),
                Upload().subcommands(
                    UploadJARCommand(factory)
                ),
                Job().subcommands(
                    StartJobCommand(factory),
                    StopJobCommand(factory),
                    ScaleJobCommand(factory),
                    GetJobDetailsCommand(factory),
                    GetJobMetricsCommand(factory)
                ),
                JobManager().subcommands(
                    GetJobManagerMetricsCommand(factory)
                ),
                TaskManager().subcommands(
                    GetTaskManagerDetailsCommand(factory),
                    GetTaskManagerMetricsCommand(factory)
                ),
                TaskManagers().subcommands(
                    ListTaskManagersCommand(factory)
                )
            ).main(args)
            System.exit(0)
        } catch (e: Exception) {
            logger.error("An error occurred while lunching the application", e)
            System.exit(-1)
        }
    }

    class MainCommand: CliktCommand(name = "flink-k8s-toolbox") {
        override fun run() = Unit
    }

    class Operator: CliktCommand(name = "operator", help = "Access operator subcommands") {
        override fun run() = Unit
    }

    class Cluster: CliktCommand(name = "cluster", help = "Access cluster subcommands") {
        override fun run() = Unit
    }

    class Upload: CliktCommand(name = "upload", help = "Access upload subcommands") {
        override fun run() = Unit
    }

    class Job: CliktCommand(name = "job", help = "Access job subcommands") {
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

    class CreateClusterCommand(private val factory: CommandFactory): CliktCommand(name = "create", help="Create a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val clusterSpec: String by option(help="The specification of the Flink cluster in JSON format").required()

        override fun run() {
//            val flinkClusterSpec = FlinkClusterSpecification.parse(Files.readString(File(System.getProperty("user.dir", ".") + "/" + clusterSpec).toPath()))
            val flinkClusterSpec = FlinkClusterSpecification.parse(Files.readString(File(clusterSpec).toPath()))
            factory.createCreateClusterCommand().run(Address(host, port), clusterName, flinkClusterSpec)
        }
    }

    class DeleteClusterCommand(private val factory: CommandFactory): CliktCommand(name = "delete", help="Delete a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createDeleteClusterCommand().run(Address(host, port), clusterName)
        }
    }

    class StartClusterCommand(private val factory: CommandFactory): CliktCommand(name="start", help="Start the cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val withoutSavepoint: Boolean by option(help="Reset savepoint when starting the job").flag(default = false)
        private val startOnlyCluster: Boolean by option(help="Create the cluster but don't run the job").flag(default = false)

        override fun run() {
            val params = StartOptions(
                withoutSavepoint = withoutSavepoint,
                startOnlyCluster = startOnlyCluster
            )
            factory.createStartClusterCommand().run(Address(host, port), clusterName, params)
        }
    }

    class StopClusterCommand(private val factory: CommandFactory): CliktCommand(name = "stop", help="Stop the cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val withSavepoint: Boolean by option(help="Create savepoint before stopping the job").flag(default = false)
        private val stopOnlyJob: Boolean by option(help="Stop the job but don't delete the cluster").flag(default = false)

        override fun run() {
            val params = StopOptions(
                withSavepoint = withSavepoint,
                stopOnlyJob = stopOnlyJob
            )
            factory.createStopClusterCommand().run(Address(host, port), clusterName, params)
        }
    }

    class StartJobCommand(private val factory: CommandFactory): CliktCommand(name="start", help="Start the job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val withoutSavepoint: Boolean by option(help="Reset savepoint when starting the job").flag(default = false)

        override fun run() {
            val params = StartOptions(
                withoutSavepoint = withoutSavepoint,
                startOnlyCluster = true
            )
            factory.createStartClusterCommand().run(Address(host, port), clusterName, params)
        }
    }

    class StopJobCommand(private val factory: CommandFactory): CliktCommand(name = "stop", help="Stop the job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val withSavepoint: Boolean by option(help="Create savepoint before stopping the job").flag(default = false)

        override fun run() {
            val params = StopOptions(
                withSavepoint = withSavepoint,
                stopOnlyJob = false
            )
            factory.createStopClusterCommand().run(Address(host, port), clusterName, params)
        }
    }

    class ScaleJobCommand(private val factory: CommandFactory): CliktCommand(name = "scale", help="Scale the job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val parallelism: Int by option(help="The parallelism of the job").int().default(1)

        override fun run() {
            val params = ScaleOptions(
                parallelism = parallelism
            )
            factory.createScaleJobCommand().run(Address(host, port), clusterName, params)
        }
    }

    class GetJobDetailsCommand(private val factory: CommandFactory): CliktCommand(name = "details", help="Get job's details") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetJobDetailsCommand().run(Address(host, port), clusterName)
        }
    }

    class GetJobMetricsCommand(private val factory: CommandFactory): CliktCommand(name = "metrics", help="Get job's metrics") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetJobMetricsCommand().run(Address(host, port), clusterName)
        }
    }

    class GetJobManagerMetricsCommand(private val factory: CommandFactory): CliktCommand(name = "metrics", help="Get JobManager's metrics") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetJobManagerMetricsCommand().run(Address(host, port), clusterName)
        }
    }

    class ListTaskManagersCommand(private val factory: CommandFactory): CliktCommand(name="list", help="List TaskManagers") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createListTaskManagersCommand().run(Address(host, port), clusterName)
        }
    }

    class GetTaskManagerDetailsCommand(private val factory: CommandFactory): CliktCommand(name = "details", help="Get TaskManager's details") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val taskmanagerId: String by option(help="The id of the TaskManager").prompt("Insert TaskManager id")

        override fun run() {
            val taskManagerId = TaskManagerId(
                taskmanagerId = taskmanagerId
            )
            factory.createGetTaskManagerDetailsCommand().run(Address(host, port), clusterName, taskManagerId)
        }
    }

    class GetTaskManagerMetricsCommand(private val factory: CommandFactory): CliktCommand(name = "metrics", help="Get TaskManager's metrics") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val taskmanagerId: String by option(help="The id of the TaskManager").prompt("Insert TaskManager id")

        override fun run() {
            val taskManagerId = TaskManagerId(
                taskmanagerId = taskmanagerId
            )
            factory.createGetTaskManagerMetricsCommand().run(Address(host, port), clusterName, taskManagerId)
        }
    }

    class RunOperatorCommand(private val factory: CommandFactory): CliktCommand(name="run", help="Run the operator") {
        private val port: Int by option(help="Listen on port").int().default(4444)
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val kubeConfig: String? by option(help="The path of Kubectl config")
        private val savepointInterval: Int by option(help="The interval between savepoints in seconds").int().default(3600)

        override fun run() {
            val config = OperatorConfig(
                port = port,
                flinkHostname = flinkHostname,
                portForward = portForward,
                namespace = namespace,
                useNodePort = kubeConfig != null,
                savepointInterval = savepointInterval
            )
            Kubernetes.configure(kubeConfig)
            factory.createRunOperatorCommand().run(config)
        }
    }

    class UploadJARCommand(private val factory: CommandFactory): CliktCommand(name="jar", help="Upload a JAR file") {
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of kuke config")
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jarPath: String by option(help="The path of the JAR file to upload").required()

        override fun run() {
            val params = UploadOptions(
                jarPath = jarPath
            )
            Kubernetes.configure(kubeConfig)
            val flinkOptions = FlinkOptions(
                hostname = flinkHostname,
                portForward = portForward,
                useNodePort = kubeConfig != null
            )
            factory.createUploadJARCommand().run(flinkOptions, namespace, clusterName, params)
        }
    }
}