package com.nextbreakpoint.flinkoperator.common

data class DeleteOptions(
    val label: String,
    val value: String,
    val limit: Int
)