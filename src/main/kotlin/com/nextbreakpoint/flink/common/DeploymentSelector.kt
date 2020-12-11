package com.nextbreakpoint.flink.common

data class DeploymentSelector(
    val namespace: String,
    val deploymentName: String
) {
    override fun toString(): String {
        return "$namespace:$deploymentName"
    }

    fun resourceName() = deploymentName
}