package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.BootstrapCommand
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.WatchAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.TaskController
import io.kubernetes.client.JSON
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

        val cache = Cache()

        val json = JSON()

        val watch = WatchAdapter(json, kubeClient, cache)

        watch.watchClusters(namespace)
        watch.watchJobs(namespace)
        watch.watchServices(namespace)
        watch.watchStatefuleSets(namespace)
        watch.watchPersistentVolumeClaims(namespace)

        while (!Thread.interrupted()) {
            logger.debug("Reconcile resource status...")

            val resources = cache.getCachedResources(clusterId)

            supervisor.execute(resources)

            TimeUnit.SECONDS.sleep(5)
        }
    }
}
