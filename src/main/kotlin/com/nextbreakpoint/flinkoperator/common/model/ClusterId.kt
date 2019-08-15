package com.nextbreakpoint.flinkoperator.common.model

data class ClusterId(
    val namespace: String,
    val name: String,
    val uuid: String
)