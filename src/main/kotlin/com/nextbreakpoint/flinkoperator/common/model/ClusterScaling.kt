package com.nextbreakpoint.flinkoperator.common.model

data class ClusterScaling(
    val taskManagers: Int,
    val taskSlots: Int
)