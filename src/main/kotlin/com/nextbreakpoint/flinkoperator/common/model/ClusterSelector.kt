package com.nextbreakpoint.flinkoperator.common.model

data class ClusterSelector(
    val namespace: String,
    val name: String,
    val uuid: String
) {
    override fun toString(): String {
        return "$namespace/$name"
    }
}