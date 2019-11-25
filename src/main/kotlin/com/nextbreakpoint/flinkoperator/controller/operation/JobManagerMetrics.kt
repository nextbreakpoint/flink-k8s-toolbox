package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.JobManagerStats
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class JobManagerMetrics(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobManagerMetrics::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val metrics = flinkClient.getJobManagerMetrics(address,
                "Status.JVM.CPU.Time,Status.JVM.CPU.Load,Status.JVM.Threads.Count,Status.JVM.Memory.Heap.Max,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Committed,Status.JVM.Memory.NonHeap.Max,Status.JVM.Memory.NonHeap.Used,Status.JVM.Memory.NonHeap.Committed,Status.JVM.Memory.Direct.Count,Status.JVM.Memory.Mapped.MemoryUsed,Status.JVM.Memory.Direct.TotalCapacity,Status.JVM.Memory.Mapped.Count,Status.JVM.Memory.Mapped.MemoryUsed,Status.JVM.Memory.Mapped.TotalCapacity,Status.JVM.GarbageCollector.Copy.Time,Status.JVM.GarbageCollector.Copy.Count,Status.JVM.GarbageCollector.MarkSweepCompact.Time,Status.JVM.GarbageCollector.MarkSweepCompact.Count,Status.JVM.ClassLoader.ClassesLoaded,Status.JVM.ClassLoader.ClassesUnloaded,taskSlotsTotal,taskSlotsAvailable,numRegisteredTaskManagers,numRunningJobs"
            )

            val metricsMap = metrics.map { metric -> metric.id to metric.value }.toMap()

            val metricsResponse = JobManagerStats(
                jvmCPUTime = metricsMap.get("Status.JVM.CPU.Time")?.toLong() ?: 0,
                jvmCPULoad = metricsMap.get("Status.JVM.CPU.Load")?.toDouble() ?: 0.0,
                jvmThreadsCount = metricsMap.get("Status.JVM.Threads.Count")?.toInt() ?: 0,
                jvmMemoryHeapMax = metricsMap.get("Status.JVM.Memory.Heap.Max")?.toLong() ?: 0L,
                jvmMemoryHeapUsed = metricsMap.get("Status.JVM.Memory.Heap.Used")?.toLong() ?: 0L,
                jvmMemoryHeapCommitted = metricsMap.get("Status.JVM.Memory.Heap.Committed")?.toLong() ?: 0L,
                jvmMemoryNonHeapMax = metricsMap.get("Status.JVM.Memory.NonHeap.Max")?.toLong() ?: 0L,
                jvmMemoryNonHeapUsed = metricsMap.get("Status.JVM.Memory.NonHeap.Used")?.toLong() ?: 0L,
                jvmMemoryNonHeapCommitted = metricsMap.get("Status.JVM.Memory.NonHeap.Committed")?.toLong() ?: 0L,
                jvmMemoryDirectCount = metricsMap.get("Status.JVM.Memory.Direct.Count")?.toInt() ?: 0,
                jvmMemoryDirectMemoryUsed = metricsMap.get("Status.JVM.Memory.Direct.MemoryUsed")?.toLong() ?: 0L,
                jvmMemoryDirectTotalCapacity = metricsMap.get("Status.JVM.Memory.Direct.TotalCapacity")?.toLong() ?: 0L,
                jvmMemoryMappedCount = metricsMap.get("Status.JVM.Memory.Mapped.Count")?.toInt() ?: 0,
                jvmMemoryMappedMemoryUsed = metricsMap.get("Status.JVM.Memory.Mapped.MemoryUsed")?.toLong() ?: 0,
                jvmMemoryMappedTotalCapacity = metricsMap.get("Status.JVM.Memory.Mapped.TotalCapacity")?.toLong() ?: 0,
                jvmGarbageCollectorCopyTime = metricsMap.get("Status.JVM.GarbageCollector.Copy.Time")?.toLong() ?: 0L,
                jvmGarbageCollectorCopyCount = metricsMap.get("Status.JVM.GarbageCollector.Copy.Count")?.toInt() ?: 0,
                jvmGarbageCollectorMarkSweepCompactTime = metricsMap.get("Status.JVM.GarbageCollector.MarkSweepCompact.Time")?.toLong() ?: 0L,
                jvmGarbageCollectorMarkSweepCompactCount = metricsMap.get("Status.JVM.GarbageCollector.MarkSweepCompact.Count")?.toInt() ?: 0,
                jvmClassLoaderClassesLoaded = metricsMap.get("Status.JVM.ClassLoader.ClassesLoaded")?.toInt() ?: 0,
                jvmClassLoaderClassesUnloaded = metricsMap.get("Status.JVM.ClassLoader.ClassesUnloaded")?.toInt() ?: 0,
                taskSlotsTotal = metricsMap.get("taskSlotsTotal")?.toInt() ?: 0,
                taskSlotsAvailable = metricsMap.get("taskSlotsAvailable")?.toInt() ?: 0,
                numRegisteredTaskManagers = metricsMap.get("numRegisteredTaskManagers")?.toInt() ?: 0,
                numRunningJobs = metricsMap.get("numRunningJobs")?.toInt() ?: 0
            )

            return Result(
                ResultStatus.SUCCESS,
                JSON().serialize(metricsResponse)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get metrics of job manager", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
    }
}