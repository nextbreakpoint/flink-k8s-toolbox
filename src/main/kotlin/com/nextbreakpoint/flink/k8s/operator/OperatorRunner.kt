package com.nextbreakpoint.flink.k8s.operator

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.MeterRegistry
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OperatorRunner(
    private val registry: MeterRegistry,
    private val controller: Controller,
    private val cache: Cache,
    private val options: RunnerOptions
) {
    companion object {
        private val logger = Logger.getLogger(OperatorRunner::class.simpleName)
    }

    fun run() {
        val gauges = registerMetrics(registry, cache.namespace)

        val operator = Operator.create(controller, cache)

        while (!Thread.interrupted()) {
            TimeUnit.SECONDS.sleep(options.pollingInterval)

            try {
                cache.updateSnapshot()
                updateMetrics(cache, gauges)
                operator.reconcile()
            } catch (e: Exception) {
                logger.error("Something went wrong", e)
            }
        }
    }

    private fun registerMetrics(registry: MeterRegistry, namespace: String): Map<ClusterStatus, AtomicInteger> {
        return ClusterStatus.values().map {
            it to registry.gauge(
                "flink_operator.clusters_status.${it.name.toLowerCase()}",
                listOf(ImmutableTag("namespace", namespace)),
                AtomicInteger(0)
            )
        }.toMap()
    }

    private fun updateMetrics(cache: Cache, gauges: Map<ClusterStatus, AtomicInteger>) {
        val clusters = cache.getFlinkClusters()

        val counters = clusters.foldRight(mutableMapOf<ClusterStatus, Int>()) { flinkCluster, counters ->
            val status = FlinkClusterStatus.getSupervisorStatus(flinkCluster)
            counters.compute(status) { _, value ->
                if (value != null) value + 1 else 1
            }
            counters
        }

        ClusterStatus.values().forEach {
            gauges[it]?.set(counters[it] ?: 0)
        }
    }
}