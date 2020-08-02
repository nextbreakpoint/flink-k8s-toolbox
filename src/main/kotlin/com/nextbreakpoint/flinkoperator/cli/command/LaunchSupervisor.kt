package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.LaunchCommand
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.SupervisorCache
import com.nextbreakpoint.flinkoperator.controller.core.SupervisorCacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Supervisor
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class LaunchSupervisor : LaunchCommand<SupervisorOptions> {
    companion object {
        private val logger = Logger.getLogger(LaunchSupervisor::class.simpleName)

        private val kubeClient = KubeClient
        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, args: SupervisorOptions) {
        val supervisorCache = SupervisorCache(namespace = namespace, clusterName = args.clusterName)

        val cacheAdapter = SupervisorCacheAdapter(kubeClient, supervisorCache)

        val controller = OperationController(flinkOptions, kubeClient = kubeClient, flinkClient = flinkClient)

        val supervisor = Supervisor.create(controller, Supervisor::class.java.name + " | " + args.clusterName)

        cacheAdapter.watchClusters(namespace)
        cacheAdapter.watchServices(namespace)
        cacheAdapter.watchJobs(namespace)
        cacheAdapter.watchPods(namespace)

        while (!Thread.interrupted()) {
            TimeUnit.SECONDS.sleep(args.pollingInterval)

            try {
                val clusterSelector = supervisorCache.findClusterSelector(namespace = namespace, name = args.clusterName)
                val resources = supervisorCache.getCachedResources(clusterSelector)
                supervisor.reconcile(clusterSelector, resources)
            } catch (e : Exception) {
                val clusterSelector = ClusterSelector(namespace = namespace, name = args.clusterName, uuid = "")
                logger.error("Error occurred while supervising cluster ${clusterSelector.name}", e)
            }
        }
    }
}
