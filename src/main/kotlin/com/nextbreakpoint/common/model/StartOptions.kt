package com.nextbreakpoint.common.model

data class StartOptions(
    val withoutSavepoint: Boolean,
    val startOnlyCluster: Boolean
)