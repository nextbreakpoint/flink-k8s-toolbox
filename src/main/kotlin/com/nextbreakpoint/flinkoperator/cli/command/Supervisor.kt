package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.BootstrapCommand
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.ClusterSupervisor
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class Supervisor : BootstrapCommand<SupervisorOptions> {
    companion object {
        private val logger = Logger.getLogger(Supervisor::class.simpleName)

        private val kubeClient = KubeClient

        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, args: SupervisorOptions) {
        val supervisor = ClusterSupervisor(kubeClient, flinkClient)

        while (!Thread.interrupted()) {
            try {
                supervisor.reconcile(flinkOptions, namespace, clusterName)
            } catch (e: Exception) {
                logger.error("Reconcile error", e)
            } finally {
                TimeUnit.SECONDS.sleep(5)
            }
        }
    }
}
