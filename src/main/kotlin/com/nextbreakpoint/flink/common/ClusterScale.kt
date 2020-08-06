package com.nextbreakpoint.flink.common

data class ClusterScale(
    val taskManagers: Int,
    val taskSlots: Int
)