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

        val loggerName = Supervisor::class.java.name + " | " + args.clusterName
        val supervisor = Supervisor.create(controller, loggerName)

        cacheAdapter.watchClusters(namespace)
        cacheAdapter.watchServices(namespace)
        cacheAdapter.watchJobs(namespace)
        cacheAdapter.watchPods(namespace)

        val clusterSelectorNoUid = ClusterSelector(namespace = namespace, name = args.clusterName, uuid = "")

        while (!Thread.interrupted()) {
            reconcile(supervisor, supervisorCache, clusterSelectorNoUid)

            TimeUnit.SECONDS.sleep(args.pollingInterval)
        }
    }

    private fun reconcile(supervisor: Supervisor, supervisorCache: SupervisorCache, clusterSelectorNoUid: ClusterSelector) {
        try {
            val clusterSelector = supervisorCache.findClusterSelector(namespace = clusterSelectorNoUid.namespace, name = clusterSelectorNoUid.name)
            if (clusterSelector != null) {
                val resources = supervisorCache.getCachedResources(clusterSelector)
                supervisor.reconcile(clusterSelector, resources)
            }
        } catch (e: Exception) {
            logger.error("Error occurred while reconciling cluster $clusterSelectorNoUid", e)
        }
    }
}
