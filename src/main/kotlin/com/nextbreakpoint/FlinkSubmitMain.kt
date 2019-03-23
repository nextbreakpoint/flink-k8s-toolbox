package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.nextbreakpoint.command.*

class FlinkSubmitMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            FlinkSubmit().subcommands(Create(), Delete(), Submit(), Cancel(), List()).main(args)
        }
    }

    class FlinkSubmit: CliktCommand() {
        override fun run() = Unit
    }

    class Create: CliktCommand(help="Create a cluster") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val dockerImage: String by option(help="The Docker image with Flink job").required()
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            CreateCluster().run(kubeConfig, dockerImage, clusterName)
        }
    }

    class Delete: CliktCommand(help="Delete a cluster") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            DeleteCluster().run(kubeConfig, clusterName)
        }
    }

    class Submit: CliktCommand(help="Submit a job") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            SubmitJob().run(kubeConfig, clusterName)
        }
    }

    class Cancel: CliktCommand(help="Cancel a job") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val jobId: String by option(help="The id of the job to cancel").required()

        override fun run() {
            CancelJob().run(kubeConfig, clusterName, jobId)
        }
    }

    class List: CliktCommand(help="List jobs") {
        private val kubeConfig: String by option(help="The path of Kubectl config").required()
        private val clusterName: String by option(help="The name of the Flink cluster").required()

        override fun run() {
            ListJobs().run(kubeConfig, clusterName)
        }
    }
}