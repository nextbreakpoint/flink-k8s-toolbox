package com.nextbreakpoint.flinkoperator.common.model

data class DeleteOptions(
    val label: String,
    val value: String,
    val limit: Int
)