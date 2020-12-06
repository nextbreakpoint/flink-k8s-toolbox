package com.nextbreakpoint.flink.common

data class JobSelector(
    val namespace: String,
    val clusterName: String,
    val jobName: String
) {
    override fun toString(): String {
        return "$namespace:$clusterName:$jobName"
    }

    val resourceName = "$clusterName-$jobName"
}