package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.flinkclient.api.FlinkApi
import com.nextbreakpoint.model.TaskManagerDescriptor
import com.nextbreakpoint.model.Metric
import io.kubernetes.client.JSON
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger
import java.util.*
import java.util.List

object TaskManagerMetricsHandler {
    private val logger = Logger.getLogger(TaskManagerMetricsHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, taskManagerDescriptor: TaskManagerDescriptor): String {
        val coreApi = CoreV1Api()

        var jobmanagerHost = "localhost"
        var jobmanagerPort = portForward ?: 8081

        if (portForward == null && useNodePort) {
            val nodes = coreApi.listNode(
                false,
                null,
                null,
                null,
                null,
                1,
                null,
                30,
                null
            )

            if (!nodes.items.isEmpty()) {
                nodes.items.get(0).status.addresses.filter {
                    it.type.equals("InternalIP")
                }.map {
                    it.address
                }.firstOrNull()?.let {
                    jobmanagerHost = it
                }
            } else {
                throw RuntimeException("Node not found")
            }
        }

        if (portForward == null) {
            val services = coreApi.listNamespacedService(
                taskManagerDescriptor.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${taskManagerDescriptor.descriptor.name},environment=${taskManagerDescriptor.descriptor.environment},role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                logger.info("Found JobManager ${service.metadata.name}")

                if (useNodePort) {
                    service.spec.ports.filter {
                        it.name.equals("ui")
                    }.filter {
                        it.nodePort != null
                    }.map {
                        it.nodePort
                    }.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                } else {
                    service.spec.ports.filter {
                        it.name.equals("ui")
                    }.filter {
                        it.port != null
                    }.map {
                        it.port
                    }.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                    jobmanagerHost = service.spec.clusterIP
                }
            } else {
                throw RuntimeException("JobManager not found")
            }
        }

        val flinkApi = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

        val response = flinkApi.getTaskManagerMetricsCall(taskManagerDescriptor.taskmanagerId, null, null, null).execute()
        if (response.isSuccessful) {
            logger.info(response.body().string())
        }
//
//        try {
//            val metrics = getMetric(
//                flinkApi,
//                "Status.JVM.CPU.Time,Status.JVM.CPU.Load,Status.JVM.Threads.Count,Status.JVM.Memory.Heap.Max,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Committed,Status.JVM.Memory.NonHeap.Max,Status.JVM.Memory.NonHeap.Used,Status.JVM.Memory.NonHeap.Committed,Status.JVM.Memory.Direct.Count,Status.JVM.Memory.Mapped.MemoryUsed,Status.JVM.Memory.Direct.TotalCapacity,Status.JVM.Memory.Mapped.Count,Status.JVM.Memory.Mapped.MemoryUsed,Status.JVM.Memory.Mapped.TotalCapacity,Status.JVM.GarbageCollector.Copy.Time,Status.JVM.GarbageCollector.Copy.Count,Status.JVM.GarbageCollector.MarkSweepCompact.Time,Status.JVM.GarbageCollector.MarkSweepCompact.Count,Status.JVM.ClassLoader.ClassesLoaded,Status.JVM.ClassLoader.ClassesUnloaded,taskSlotsTotal,taskSlotsAvailable,numRegisteredTaskManagers,numRunningJobs"
//            )
//
//            val metricsMap = metrics.map { metric -> metric.id to metric.value }.toMap()
//
//            val metricsResponse = JobManagerMetrics(
//                jvmCPUTime = metricsMap.get("Status.JVM.CPU.Time")?.toLong() ?: 0,
//                jvmCPULoad = metricsMap.get("Status.JVM.CPU.Load")?.toDouble() ?: 0.0,
//                jvmThreadsCount = metricsMap.get("Status.JVM.Threads.Count")?.toInt() ?: 0,
//                jvmMemoryHeapMax = metricsMap.get("Status.JVM.Memory.Heap.Max")?.toLong() ?: 0L,
//                jvmMemoryHeapUsed = metricsMap.get("Status.JVM.Memory.Heap.Used")?.toLong() ?: 0L,
//                jvmMemoryHeapCommitted = metricsMap.get("Status.JVM.Memory.Heap.Committed")?.toLong() ?: 0L,
//                jvmMemoryNonHeapMax = metricsMap.get("Status.JVM.Memory.NonHeap.Max")?.toLong() ?: 0L,
//                jvmMemoryNonHeapUsed = metricsMap.get("Status.JVM.Memory.NonHeap.Used")?.toLong() ?: 0L,
//                jvmMemoryNonHeapCommitted = metricsMap.get("Status.JVM.Memory.NonHeap.Committed")?.toLong() ?: 0L,
//                jvmMemoryDirectCount = metricsMap.get("Status.JVM.Memory.Direct.Count")?.toInt() ?: 0,
//                jvmMemoryDirectMemoryUsed = metricsMap.get("Status.JVM.Memory.Direct.MemoryUsed")?.toLong() ?: 0L,
//                jvmMemoryDirectTotalCapacity = metricsMap.get("Status.JVM.Memory.Direct.TotalCapacity")?.toLong() ?: 0L,
//                jvmMemoryMappedCount = metricsMap.get("Status.JVM.Memory.Mapped.Count")?.toInt() ?: 0,
//                jvmMemoryMappedMemoryUsed = metricsMap.get("Status.JVM.Memory.Mapped.MemoryUsed")?.toLong() ?: 0,
//                jvmMemoryMappedTotalCapacity = metricsMap.get("Status.JVM.Memory.Mapped.TotalCapacity")?.toLong() ?: 0,
//                jvmGarbageCollectorCopyTime = metricsMap.get("Status.JVM.GarbageCollector.Copy.Time")?.toLong() ?: 0L,
//                jvmGarbageCollectorCopyCount = metricsMap.get("Status.JVM.GarbageCollector.Copy.Count")?.toInt() ?: 0,
//                jvmGarbageCollectorMarkSweepCompactTime = metricsMap.get("Status.JVM.GarbageCollector.MarkSweepCompact.Time")?.toLong() ?: 0L,
//                jvmGarbageCollectorMarkSweepCompactCount = metricsMap.get("Status.JVM.GarbageCollector.MarkSweepCompact.Count")?.toInt() ?: 0,
//                jvmClassLoaderClassesLoaded = metricsMap.get("Status.JVM.ClassLoader.ClassesLoaded")?.toInt() ?: 0,
//                jvmClassLoaderClassesUnloaded = metricsMap.get("Status.JVM.ClassLoader.ClassesUnloaded")?.toInt() ?: 0,
//                taskSlotsTotal = metricsMap.get("taskSlotsTotal")?.toInt() ?: 0,
//                taskSlotsAvailable = metricsMap.get("taskSlotsAvailable")?.toInt() ?: 0,
//                numRegisteredTaskManagers = metricsMap.get("numRegisteredTaskManagers")?.toInt() ?: 0,
//                numRunningJobs = metricsMap.get("numRunningJobs")?.toInt() ?: 0
//            )
//
//            return Gson().toJson(metricsResponse)
//        } catch (e : Exception) {
//            e.printStackTrace()
//        }

        return Gson().toJson(mapOf<String, String>())
    }

    private fun getMetric(flinkApi: FlinkApi, metricKey: String): List<Metric> {
        val response = flinkApi.getJobManagerMetricsCall(metricKey, null, null).execute()
        return if (response.isSuccessful) {
//            logger.info(response.body().string())
            response.body().use {
                JSON().deserialize(it.source().readUtf8Line(), object : TypeToken<List<Metric>>() {}.type) as List<Metric>
            }
        } else {
            LinkedList<Metric>() as List<Metric>
        }
    }
}