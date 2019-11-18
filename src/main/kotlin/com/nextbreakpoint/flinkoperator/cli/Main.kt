package com.nextbreakpoint.flinkoperator.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.flinkoperator.common.model.BootstrapOptions
import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorConfig
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import org.apache.log4j.Logger
import java.io.File
import java.nio.file.Files

class Main(private val factory: CommandFactory) {
    companion object {
        private val logger = Logger.getLogger(Main::class.simpleName)

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.Log4j2LogDelegateFactory")

                System.setProperty("crypto.policy", "unlimited")

                Main(DefaultCommandFactory).run(args)

                System.exit(0)
            } catch (e: Exception) {
                logger.error("Failure", e)

                System.exit(1)
            }
        }
    }

    fun run(args: Array<String>) {
        MainCommand().subcommands(
            Operator().subcommands(
                RunOperatorCommand(factory)
            ),
            Cluster().subcommands(
                CreateClusterCommand(factory),
                DeleteClusterCommand(factory),
                GetClusterStatusCommand(factory),
                StartClusterCommand(factory),
                StopClusterCommand(factory),
                ScaleClusterCommand(factory)
            ),
            Savepoint().subcommands(
                TriggerSavepointCommand(factory)
            ),
            Bootstrap().subcommands(
                BootstrapCommand(factory)
            ),
            Job().subcommands(
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

    class Savepoint: CliktCommand(name = "savepoint", help = "Access savepoint subcommands") {
        override fun run() = Unit
    }

    class Bootstrap: CliktCommand(name = "bootstrap", help = "Access bootstrap subcommands") {
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
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val clusterSpec: String by option(help="The specification of the Flink cluster in JSON format").required()

        override fun run() {
            factory.createCreateClusterCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName, String(Files.readAllBytes(File(clusterSpec).toPath())))
        }
    }

    class DeleteClusterCommand(private val factory: CommandFactory): CliktCommand(name = "delete", help="Delete a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createDeleteClusterCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class GetClusterStatusCommand(private val factory: CommandFactory): CliktCommand(name="status", help="Get cluster's status") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetClusterStatusCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class StartClusterCommand(private val factory: CommandFactory): CliktCommand(name="start", help="Start a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val withoutSavepoint: Boolean by option(help="Reset savepoint when starting the job").flag(default = false)

        override fun run() {
            val params = StartOptions(
                withoutSavepoint = withoutSavepoint
            )
            factory.createStartClusterCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName, params)
        }
    }

    class StopClusterCommand(private val factory: CommandFactory): CliktCommand(name = "stop", help="Stop a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val withoutSavepoint: Boolean by option(help="Skip savepoint when stopping the job").flag(default = false)
        private val deleteResources: Boolean by option(help="Delete the cluster's resources").flag(default = false)

        override fun run() {
            val params = StopOptions(
                withoutSavepoint = withoutSavepoint,
                deleteResources = deleteResources
            )
            factory.createStopClusterCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName, params)
        }
    }

    class ScaleClusterCommand(private val factory: CommandFactory): CliktCommand(name = "scale", help="Scale a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val taskManagers: Int by option(help="Number of Task Managers").int().required()

        override fun run() {
            val params = ScaleOptions(
                taskManagers = taskManagers
            )
            factory.createScaleClusterCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName, params)
        }
    }

    class TriggerSavepointCommand(private val factory: CommandFactory): CliktCommand(name="trigger", help="Trigger a new savepoint") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createTriggerSavepointCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class GetJobDetailsCommand(private val factory: CommandFactory): CliktCommand(name = "details", help="Get job's details") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetJobDetailsCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class GetJobMetricsCommand(private val factory: CommandFactory): CliktCommand(name = "metrics", help="Get job's metrics") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetJobMetricsCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class GetJobManagerMetricsCommand(private val factory: CommandFactory): CliktCommand(name = "metrics", help="Get JobManager's metrics") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createGetJobManagerMetricsCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class ListTaskManagersCommand(private val factory: CommandFactory): CliktCommand(name="list", help="Get list of TaskManagers") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            factory.createListTaskManagersCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName)
        }
    }

    class GetTaskManagerDetailsCommand(private val factory: CommandFactory): CliktCommand(name = "details", help="Get TaskManager's details") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val taskmanagerId: String by option(help="The id of the TaskManager").prompt("Insert TaskManager id")

        override fun run() {
            val taskManagerId = TaskManagerId(
                taskmanagerId = taskmanagerId
            )
            factory.createGetTaskManagerDetailsCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName, taskManagerId)
        }
    }

    class GetTaskManagerMetricsCommand(private val factory: CommandFactory): CliktCommand(name = "metrics", help="Get TaskManager's metrics") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val taskmanagerId: String by option(help="The id of the TaskManager").prompt("Insert TaskManager id")

        override fun run() {
            val taskManagerId = TaskManagerId(
                taskmanagerId = taskmanagerId
            )
            factory.createGetTaskManagerMetricsCommand().run(
                ConnectionConfig(
                    host,
                    port,
                    keystorePath,
                    keystoreSecret,
                    truststorePath,
                    truststoreSecret
                ), clusterName, taskManagerId)
        }
    }

    class RunOperatorCommand(private val factory: CommandFactory): CliktCommand(name="run", help="Run the Flink Operator") {
        private val port: Int by option(help="Listen on port").int().default(4444)
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val kubeConfig: String? by option(help="The path of Kubectl config")
        private val keystorePath: String? by option(help="The operator's keystore path")
        private val keystoreSecret: String? by option(help="The operator's keystore secret")
        private val truststorePath: String? by option(help="The operator's truststore path")
        private val truststoreSecret: String? by option(help="The operator's truststore secret")

        override fun run() {
            val config = OperatorConfig(
                port = port,
                flinkHostname = flinkHostname,
                portForward = portForward,
                namespace = namespace,
                useNodePort = kubeConfig != null,
                keystorePath = keystorePath,
                keystoreSecret = keystoreSecret,
                truststorePath = truststorePath,
                truststoreSecret = truststoreSecret
            )
            KubeClient.configure(kubeConfig)
            factory.createRunOperatorCommand().run(config)
        }
    }

    class BootstrapCommand(private val factory: CommandFactory): CliktCommand(name="upload", help="Upload a JAR file") {
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of kuke config")
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jarPath: String by option(help="The path of the JAR file to upload").required()

        override fun run() {
            val params = BootstrapOptions(
                jarPath = jarPath
            )
            KubeClient.configure(kubeConfig)
            val flinkOptions = FlinkOptions(
                hostname = flinkHostname,
                portForward = portForward,
                useNodePort = kubeConfig != null
            )
            factory.createBootstrapCommand().run(flinkOptions, namespace, clusterName, params)
        }
    }
}
