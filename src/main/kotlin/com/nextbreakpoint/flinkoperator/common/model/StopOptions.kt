package com.nextbreakpoint.flinkoperator.common.model

data class StopOptions(
    val withoutSavepoint: Boolean,
    val deleteResources: Boolean
)