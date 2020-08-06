package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class SupervisorRunner(
    private val controller: Controller,
    private val cache: Cache,
    private val options: RunnerOptions
) {
    companion object {
        private val logger = Logger.getLogger(SupervisorRunner::class.simpleName)
    }

    fun run() {
        val supervisor = Supervisor.create(controller, cache, options.taskTimeout)

        while (!Thread.interrupted()) {
            TimeUnit.SECONDS.sleep(options.pollingInterval)

            try {
                cache.updateSnapshot()
                reconcileResources(cache, supervisor)
                supervisor.cleanup()
            } catch (e: Exception) {
                logger.error("Something went wrong", e)
            }
        }
    }

    private fun reconcileResources(cache: Cache, supervisor: Supervisor) {
        cache.findClusterSelector(
            namespace = cache.namespace,
            name = cache.clusterName
        )?.let {
            reconcileResources(supervisor, it)
        }
    }

    private fun reconcileResources(supervisor: Supervisor, clusterSelector: ResourceSelector) {
        supervisor.reconcile(clusterSelector)
    }
}