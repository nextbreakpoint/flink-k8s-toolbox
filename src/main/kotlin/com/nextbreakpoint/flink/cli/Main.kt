package com.nextbreakpoint.flink.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long
import com.nextbreakpoint.flink.cli.factory.CommandDefaultFactory
import com.nextbreakpoint.flink.cli.factory.CommandFactory
import com.nextbreakpoint.flink.common.BootstrapOptions
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.OperatorOptions
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.SupervisorOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.common.KubeClient
import java.io.File
import java.nio.file.Files
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess


class Main(private val factory: CommandFactory = CommandDefaultFactory) {
    companion object {
        private val logger = Logger.getLogger(Main::class.simpleName)

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                System.setProperty("java.util.logging.SimpleFormatter.format", "%1\$tY-%1\$tm-%1\$td %1\$tH:%1\$tM:%1\$tS %4\$s %3\$s - %5\$s%6\$s%n")

                Main().run(args)

                exitProcess(0)
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Failure", e)

                exitProcess(1)
            }
        }
    }

    fun run(args: Array<String>) {
        MainCommand().subcommands(
            Operator().subcommands(
                LaunchOperatorCommand(factory)
            ),
            Supervisor().subcommands(
                LaunchSupervisorCommand(factory)
            ),
            Bootstrap().subcommands(
                LaunchBootstrapCommand(factory)
            ),
            Deployments().subcommands(
                ListDeploymentsCommand(factory)
            ),
            Deployment().subcommands(
                CreateDeploymentCommand(factory),
                DeleteDeploymentCommand(factory),
                UpdateDeploymentCommand(factory),
                GetDeploymentStatusCommand(factory)
            ),
            Clusters().subcommands(
                ListClustersCommand(factory)
            ),
            Cluster().subcommands(
                CreateClusterCommand(factory),
                DeleteClusterCommand(factory),
                UpdateClusterCommand(factory),
                GetClusterStatusCommand(factory),
                StartClusterCommand(factory),
                StopClusterCommand(factory),
                ScaleClusterCommand(factory)
            ),
            Savepoint().subcommands(
                TriggerSavepointCommand(factory),
                ForgetSavepointCommand(factory)
            ),
            Jobs().subcommands(
                ListJobsCommand(factory)
            ),
            Job().subcommands(
                CreateJobCommand(factory),
                DeleteJobCommand(factory),
                UpdateJobCommand(factory),
                GetJobDetailsCommand(factory),
                GetJobMetricsCommand(factory),
                GetJobStatusCommand(factory),
                StartJobCommand(factory),
                StopJobCommand(factory),
                ScaleJobCommand(factory)
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

    class MainCommand: CliktCommand(name = "flinkctl") {
        override fun run() = Unit
    }

    class Operator: CliktCommand(name = "operator", help = "Access operator subcommands") {
        override fun run() = Unit
    }

    class Supervisor: CliktCommand(name = "supervisor", help = "Access supervisor subcommands") {
        override fun run() = Unit
    }

    class Bootstrap: CliktCommand(name = "bootstrap", help = "Access bootstrap subcommands") {
        override fun run() = Unit
    }

    class Deployments: CliktCommand(name = "deployments", help = "Access deployments subcommands") {
        override fun run() = Unit
    }

    class Deployment: CliktCommand(name = "deployment", help = "Access deployment subcommands") {
        override fun run() = Unit
    }

    class Clusters: CliktCommand(name = "clusters", help = "Access clusters subcommands") {
        override fun run() = Unit
    }

    class Cluster: CliktCommand(name = "cluster", help = "Access cluster subcommands") {
        override fun run() = Unit
    }

    class Savepoint: CliktCommand(name = "savepoint", help = "Access savepoint subcommands") {
        override fun run() = Unit
    }

    class Jobs: CliktCommand(name = "jobs", help = "Access jobs subcommands") {
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

    class ListDeploymentsCommand(private val factory: CommandFactory): CliktCommand(name = "list", help="List deployments") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createListDeploymentsCommand().run(
                connectionConfig
            )
        }
    }

    class CreateDeploymentCommand(private val factory: CommandFactory): CliktCommand(name = "create", help="Create a deployment") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val deploymentName: String by option(help="The name of the Flink deployment").required()
        private val deploymentSpec: String by option(help="The specification of the Flink deployment in JSON format").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createCreateDeploymentCommand().run(
                connectionConfig, deploymentName, String(Files.readAllBytes(File(deploymentSpec).toPath()))
            )
        }
    }

    class DeleteDeploymentCommand(private val factory: CommandFactory): CliktCommand(name = "delete", help="Delete a deployment") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val deploymentName: String by option(help="The name of the Flink deployment").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createDeleteDeploymentCommand().run(
                connectionConfig, deploymentName, null
            )
        }
    }

    class UpdateDeploymentCommand(private val factory: CommandFactory): CliktCommand(name = "update", help="Update a deployment") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val deploymentName: String by option(help="The name of the Flink deployment").required()
        private val deploymentSpec: String by option(help="The specification of the Flink deployment in JSON format").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createUpdateDeploymentCommand().run(
                connectionConfig, deploymentName, String(Files.readAllBytes(File(deploymentSpec).toPath()))
            )
        }
    }

    class GetDeploymentStatusCommand(private val factory: CommandFactory): CliktCommand(name="status", help="Get deployment's status") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val deploymentName: String by option(help="The name of the Flink deployment").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetDeploymentStatusCommand().run(
                connectionConfig, deploymentName, null
            )
        }
    }

    class ListClustersCommand(private val factory: CommandFactory): CliktCommand(name = "list", help="List clusters") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createListClustersCommand().run(
                connectionConfig
            )
        }
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createCreateClusterCommand().run(
                connectionConfig, clusterName, String(Files.readAllBytes(File(clusterSpec).toPath()))
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createDeleteClusterCommand().run(
                connectionConfig, clusterName, null
            )
        }
    }

    class UpdateClusterCommand(private val factory: CommandFactory): CliktCommand(name = "update", help="Update a cluster") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val clusterSpec: String by option(help="The specification of the Flink cluster in JSON format").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createUpdateClusterCommand().run(
                connectionConfig, clusterName, String(Files.readAllBytes(File(clusterSpec).toPath()))
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetClusterStatusCommand().run(
                connectionConfig, clusterName, null
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createStartClusterCommand().run(
                connectionConfig, clusterName, params
            )
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

        override fun run() {
            val params = StopOptions(
                withoutSavepoint = withoutSavepoint
            )
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createStopClusterCommand().run(
                connectionConfig, clusterName, params
            )
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
            val params = ScaleClusterOptions(
                taskManagers = taskManagers
            )
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createScaleClusterCommand().run(
                connectionConfig, clusterName, params
            )
        }
    }

    class ListJobsCommand(private val factory: CommandFactory): CliktCommand(name = "list", help="List jobs") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createListJobsCommand().run(
                connectionConfig, clusterName, null
            )
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
        private val jobName: String by option(help="The name of the Flink job").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createTriggerSavepointCommand().run(
                connectionConfig, clusterName, jobName, null
            )
        }
    }

    class ForgetSavepointCommand(private val factory: CommandFactory): CliktCommand(name="forget", help="Forget savepoint reference") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createForgetSavepointCommand().run(
                connectionConfig, clusterName, jobName, null
            )
        }
    }

    class CreateJobCommand(private val factory: CommandFactory): CliktCommand(name = "create", help="Create a job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()
        private val jobSpec: String by option(help="The specification of the Flink job in JSON format").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createCreateJobCommand().run(
                connectionConfig, clusterName, jobName, String(Files.readAllBytes(File(jobSpec).toPath()))
            )
        }
    }

    class DeleteJobCommand(private val factory: CommandFactory): CliktCommand(name = "delete", help="Delete a job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createDeleteJobCommand().run(
                connectionConfig, clusterName, jobName, null
            )
        }
    }

    class UpdateJobCommand(private val factory: CommandFactory): CliktCommand(name = "update", help="Update a job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()
        private val jobSpec: String by option(help="The specification of the Flink job in JSON format").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createUpdateJobCommand().run(
                connectionConfig, clusterName, jobName, String(Files.readAllBytes(File(jobSpec).toPath()))
            )
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
        private val jobName: String by option(help="The name of the Flink job").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetJobDetailsCommand().run(
                connectionConfig, clusterName, jobName, null
            )
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
        private val jobName: String by option(help="The name of the Flink job").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetJobMetricsCommand().run(
                connectionConfig, clusterName, jobName, null
            )
        }
    }

    class GetJobStatusCommand(private val factory: CommandFactory): CliktCommand(name="status", help="Get job's status") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()

        override fun run() {
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetJobStatusCommand().run(
                connectionConfig, clusterName, jobName, null
            )
        }
    }

    class StartJobCommand(private val factory: CommandFactory): CliktCommand(name = "start", help="Start a job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()
        private val withoutSavepoint: Boolean by option(help="Reset savepoint when starting the job").flag(default = false)

        override fun run() {
            val params = StartOptions(
                withoutSavepoint = withoutSavepoint
            )
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createStartJobCommand().run(
                connectionConfig, clusterName, jobName, params
            )
        }
    }

    class StopJobCommand(private val factory: CommandFactory): CliktCommand(name = "stop", help="Stop a job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()
        private val withoutSavepoint: Boolean by option(help="Skip savepoint when stopping the job").flag(default = false)

        override fun run() {
            val params = StopOptions(
                withoutSavepoint = withoutSavepoint
            )
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createStopJobCommand().run(
                connectionConfig, clusterName, jobName, params
            )
        }
    }

    class ScaleJobCommand(private val factory: CommandFactory): CliktCommand(name = "scale", help="Scale a job") {
        private val host: String by option(help="The operator host").default("localhost")
        private val port: Int by option(help="The operator port").int().default(4444)
        private val keystorePath: String? by option(help="The keystore path")
        private val keystoreSecret: String? by option(help="The keystore secret")
        private val truststorePath: String? by option(help="The truststore path")
        private val truststoreSecret: String? by option(help="The truststore secret")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()
        private val parallelism: Int by option(help="Max parallelism").int().required()

        override fun run() {
            val params = ScaleJobOptions(
                parallelism = parallelism
            )
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createScaleJobCommand().run(
                connectionConfig, clusterName, jobName, params
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetJobManagerMetricsCommand().run(
                connectionConfig, clusterName, null
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createListTaskManagersCommand().run(
                connectionConfig, clusterName, null
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetTaskManagerDetailsCommand().run(
                connectionConfig, clusterName, taskManagerId
            )
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
            val connectionConfig = ConnectionConfig(
                host,
                port,
                keystorePath,
                keystoreSecret,
                truststorePath,
                truststoreSecret
            )
            factory.createGetTaskManagerMetricsCommand().run(
                connectionConfig, clusterName, taskManagerId
            )
        }
    }

    class LaunchOperatorCommand(private val factory: CommandFactory): CliktCommand(name="run", help="Execute operator process") {
        private val port: Int by option(help="Listen on port").int().default(4444)
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val kubeConfig: String? by option(help="The path of Kubectl config")
        private val keystorePath: String? by option(help="The operator's keystore path")
        private val keystoreSecret: String? by option(help="The operator's keystore secret")
        private val truststorePath: String? by option(help="The operator's truststore path")
        private val truststoreSecret: String? by option(help="The operator's truststore secret")
        private val pollingInterval: Long by option(help="The polling interval in seconds").long().default(10)
        private val taskTimeout: Long by option(help="The task timeout in seconds").long().default(120)
        private val dryRun: Boolean by option(help="Run in dry-run mode").flag(default = false)

        override fun run() {
            val params = OperatorOptions(
                port = port,
                keystorePath = keystorePath,
                keystoreSecret = keystoreSecret,
                truststorePath = truststorePath,
                truststoreSecret = truststoreSecret,
                pollingInterval = pollingInterval,
                taskTimeout = taskTimeout,
                dryRun = dryRun
            )
            KubeClient.configure(kubeConfig)
            val flinkOptions = FlinkOptions(
                hostname = flinkHostname,
                portForward = portForward,
                useNodePort = kubeConfig != null
            )
            factory.createLaunchOperatorCommand().run(
                flinkOptions, namespace, params
            )
        }
    }

    class LaunchSupervisorCommand(private val factory: CommandFactory): CliktCommand(name="run", help="Execute supervisor process") {
        private val port: Int by option(help="Listen on port").int().default(4445)
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of Kubernetes config")
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val keystorePath: String? by option(help="The supervisor's keystore path")
        private val keystoreSecret: String? by option(help="The supervisor's keystore secret")
        private val truststorePath: String? by option(help="The supervisor's truststore path")
        private val truststoreSecret: String? by option(help="The supervisor's truststore secret")
        private val pollingInterval: Long by option(help="The polling interval in seconds").long().default(10)
        private val taskTimeout: Long by option(help="The task timeout in seconds").long().default(300)
        private val dryRun: Boolean by option(help="Run in dry-run mode").flag(default = false)

        override fun run() {
            val params = SupervisorOptions(
                port = port,
                keystorePath = keystorePath,
                keystoreSecret = keystoreSecret,
                truststorePath = truststorePath,
                truststoreSecret = truststoreSecret,
                clusterName = clusterName,
                pollingInterval = pollingInterval,
                taskTimeout = taskTimeout,
                dryRun = dryRun
            )
            KubeClient.configure(kubeConfig)
            val flinkOptions = FlinkOptions(
                hostname = flinkHostname,
                portForward = portForward,
                useNodePort = kubeConfig != null
            )
            factory.createLaunchSupervisorCommand().run(
                flinkOptions, namespace, params
            )
        }
    }

    class LaunchBootstrapCommand(private val factory: CommandFactory): CliktCommand(name="run", help="Execute bootstrap process") {
        private val flinkHostname: String? by option(help="The hostname of the JobManager")
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of Kubernetes config")
        private val namespace: String by option(help="The namespace of the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobName: String by option(help="The name of the Flink job").required()
        private val jarPath: String by option(help="The path of the JAR file to upload").required()
        private val className: String by option(help="The name of the class to execute").required()
        private val parallelism: Int by option(help="The default parallelism of the job").int().default(1)
        private val savepointPath: String? by option(help="The path of a valid savepoint")
        private val argument: List<String> by option(help="One or more job's argument").multiple()
        private val dryRun: Boolean by option(help="Run in dry-run mode").flag(default = false)

        override fun run() {
            val params = BootstrapOptions(
                clusterName = clusterName,
                jobName = jobName,
                jarPath = jarPath,
                className = className,
                parallelism = parallelism,
                savepointPath = savepointPath,
                arguments = argument,
                dryRun = dryRun
            )
            KubeClient.configure(kubeConfig)
            val flinkOptions = FlinkOptions(
                hostname = flinkHostname,
                portForward = portForward,
                useNodePort = kubeConfig != null
            )
            factory.createLaunchBootstrapCommand().run(
                flinkOptions, namespace, params
            )
        }
    }
}
