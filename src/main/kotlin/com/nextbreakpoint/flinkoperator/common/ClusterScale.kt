package com.nextbreakpoint.flinkoperator.common

data class ClusterScale(
    val taskManagers: Int,
    val taskSlots: Int
)