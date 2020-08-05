package com.nextbreakpoint.flinkoperator.common

data class StopOptions(
    val withoutSavepoint: Boolean,
    val deleteResources: Boolean
)