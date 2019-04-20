package com.nextbreakpoint.model

class JobManagerMetrics(
    val jvmCPUTime: Long,
    val jvmCPULoad: Double,
    val jvmThreadsCount: Int,
    val jvmMemoryHeapMax: Long,
    val jvmMemoryHeapUsed: Long,
    val jvmMemoryHeapCommitted: Long,
    val jvmMemoryNonHeapMax: Long,
    val jvmMemoryNonHeapUsed: Long,
    val jvmMemoryNonHeapCommitted: Long,
    val jvmMemoryDirectCount: Int,
    val jvmMemoryDirectMemoryUsed: Long,
    val jvmMemoryDirectTotalCapacity: Long,
    val jvmMemoryMappedCount: Int,
    val jvmMemoryMappedMemoryUsed: Long,
    val jvmMemoryMappedTotalCapacity: Long,
    val jvmGarbageCollectorCopyTime: Long,
    val jvmGarbageCollectorCopyCount: Int,
    val jvmGarbageCollectorMarkSweepCompactTime: Long,
    val jvmGarbageCollectorMarkSweepCompactCount: Int,
    val jvmClassLoaderClassesLoaded: Int,
    val jvmClassLoaderClassesUnloaded: Int,
    val taskSlotsTotal: Int,
    val taskSlotsAvailable: Int,
    val numRegisteredTaskManagers: Int,
    val numRunningJobs: Int
)
