package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.BootstrapCommand
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
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

        val clusterSelector = ClusterSelector(namespace = namespace, name = clusterName, uuid = "")

        val supervisor = TaskController.create(controller, clusterSelector)

        val cache = Cache()

        val watch = CacheAdapter(kubeClient, cache)

        watch.watchClusters(namespace)
        watch.watchJobs(namespace)
        watch.watchServices(namespace)
        watch.watchStatefuleSets(namespace)
        watch.watchPersistentVolumeClaims(namespace)

        while (!Thread.interrupted()) {
            logger.info("Reconciling ${clusterSelector}...")

            val resources = cache.getCachedResources(clusterSelector)

            supervisor.execute(resources)

            TimeUnit.SECONDS.sleep(5)
        }
    }
}
