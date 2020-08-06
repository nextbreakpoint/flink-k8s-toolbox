package com.nextbreakpoint.flink.common

data class ResourceSelector(
    val namespace: String,
    val name: String,
    val uid: String
) {
    override fun toString(): String {
        return "$namespace/$name"
    }
}