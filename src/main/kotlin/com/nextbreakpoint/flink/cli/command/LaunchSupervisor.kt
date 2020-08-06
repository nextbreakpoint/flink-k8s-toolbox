package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.LaunchCommand
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.common.SupervisorOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.CacheAdapter
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.SupervisorRunner

class LaunchSupervisor : LaunchCommand<SupervisorOptions> {
    companion object {
        private val kubeClient = KubeClient
        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, options: SupervisorOptions) {
        val cache = Cache(namespace = namespace, clusterName = options.clusterName)

        val cacheAdapter = CacheAdapter(kubeClient, cache)

        cacheAdapter.watchFlinkClusters(namespace)
        cacheAdapter.watchFlinkJobs(namespace)
        cacheAdapter.watchServices(namespace)
        cacheAdapter.watchJobs(namespace)
        cacheAdapter.watchPods(namespace)

        val controller = Controller(flinkOptions, flinkClient, kubeClient, options.dryRun)

        val runner = SupervisorRunner(controller, cache, RunnerOptions(pollingInterval = options.pollingInterval, taskTimeout = options.taskTimeout))

        runner.run()
    }
}
