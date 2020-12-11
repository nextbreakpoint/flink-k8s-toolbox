package com.nextbreakpoint.flink.k8s.operator

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Level
import java.util.logging.Logger

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
        val clusterGauges = registerClusterMetrics(registry, cache.namespace)

        val jobGauges = registerJobMetrics(registry, cache.namespace)

        val operator = Operator.create(controller, cache, options.taskTimeout, options.pollingInterval, options.serverConfig)

        while (!Thread.interrupted()) {
            TimeUnit.SECONDS.sleep(options.pollingInterval)

            try {
                cache.updateSnapshot()
                updateClusterMetrics(cache, clusterGauges)
                updateJobMetrics(cache, jobGauges)
                operator.reconcile()
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Something went wrong", e)
            }
        }
    }

    private fun registerClusterMetrics(registry: MeterRegistry, namespace: String): Map<ClusterStatus, AtomicInteger> {
        return ClusterStatus.values().map {
            it to registry.gauge(
                "flink_operator.cluster_status",
                listOf(ImmutableTag("namespace", namespace), ImmutableTag("status", it.name)),
                AtomicInteger(0)
            )
        }.toMap()
    }

    private fun registerJobMetrics(registry: MeterRegistry, namespace: String): Map<JobStatus, AtomicInteger> {
        return JobStatus.values().map {
            it to registry.gauge(
                "flink_operator.job_status",
                listOf(ImmutableTag("namespace", namespace), ImmutableTag("status", it.name)),
                AtomicInteger(0)
            )
        }.toMap()
    }

    private fun updateClusterMetrics(cache: Cache, clusterGauges: Map<ClusterStatus, AtomicInteger>) {
        val clusterCounters = cache.getFlinkClusters().foldRight(mutableMapOf<ClusterStatus, Int>()) { flinkCluster, counters ->
            val status = FlinkClusterStatus.getSupervisorStatus(flinkCluster)
            counters.compute(status) { _, value ->
                if (value != null) value + 1 else 1
            }
            counters
        }

        ClusterStatus.values().forEach {
            clusterGauges[it]?.set(clusterCounters[it] ?: 0)
        }
    }

    private fun updateJobMetrics(cache: Cache, jobGauges: Map<JobStatus, AtomicInteger>) {
        val jobCounters = cache.getFlinkJobs().foldRight(mutableMapOf<JobStatus, Int>()) { flinkCluster, counters ->
            val status = FlinkJobStatus.getSupervisorStatus(flinkCluster)
            counters.compute(status) { _, value ->
                if (value != null) value + 1 else 1
            }
            counters
        }

        JobStatus.values().forEach {
            jobGauges[it]?.set(jobCounters[it] ?: 0)
        }
    }
}