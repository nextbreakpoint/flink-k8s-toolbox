package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.BootstrapCommand
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.TaskController
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class Supervisor : BootstrapCommand<SupervisorOptions> {
    companion object {
        private val logger = Logger.getLogger(Supervisor::class.simpleName)

        private val kubeClient = KubeClient

        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, args: SupervisorOptions) {
        val controller = OperationController(flinkOptions, kubeClient = kubeClient, flinkClient = flinkClient)

        val clusterId = ClusterId(namespace = namespace, name = clusterName, uuid = "")

        val supervisor = TaskController(controller, clusterId)

        while (!Thread.interrupted()) {
            val clusters = controller.findClusters(clusterId.namespace, clusterId.name)

            if (clusters.items.size != 1) {
                logger.error("Can't find cluster resource ${clusterId.name}")

                return
            }

            logger.debug("Reconcile resource status...")

            val cluster = clusters.items.first()

            supervisor.execute(cluster)

            TimeUnit.SECONDS.sleep(5)
        }
    }
}
