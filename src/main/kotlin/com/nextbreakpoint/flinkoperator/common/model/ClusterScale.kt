package com.nextbreakpoint.flinkoperator.common.model

data class ClusterScale(
    val taskManagers: Int,
    val taskSlots: Int
)