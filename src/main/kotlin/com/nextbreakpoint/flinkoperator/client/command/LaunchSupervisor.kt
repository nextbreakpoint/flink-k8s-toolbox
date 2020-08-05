package com.nextbreakpoint.flinkoperator.client.command

import com.nextbreakpoint.flinkoperator.client.core.LaunchCommand
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.SupervisorOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.supervisor.core.Cache
import com.nextbreakpoint.flinkoperator.server.supervisor.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.server.controller.Controller
import com.nextbreakpoint.flinkoperator.server.supervisor.Supervisor
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class LaunchSupervisor : LaunchCommand<SupervisorOptions> {
    companion object {
        private val logger = Logger.getLogger(LaunchSupervisor::class.simpleName)

        private val kubeClient = KubeClient
        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, args: SupervisorOptions) {
        val supervisorCache = Cache(namespace = namespace, clusterName = args.clusterName)

        val cacheAdapter = CacheAdapter(kubeClient, supervisorCache)

        val controller = Controller(flinkOptions, kubeClient = kubeClient, flinkClient = flinkClient)

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

    private fun reconcile(supervisor: Supervisor, cache: Cache, clusterSelectorNoUid: ClusterSelector) {
        try {
            val clusterSelector = cache.findClusterSelector(namespace = clusterSelectorNoUid.namespace, name = clusterSelectorNoUid.name)
            if (clusterSelector != null) {
                val resources = cache.getCachedResources(clusterSelector)
                supervisor.reconcile(clusterSelector, resources)
            }
        } catch (e: Exception) {
            logger.error("Error occurred while reconciling cluster $clusterSelectorNoUid", e)
        }
    }
}
