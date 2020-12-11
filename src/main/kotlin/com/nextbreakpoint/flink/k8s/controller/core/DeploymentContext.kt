package com.nextbreakpoint.flink.k8s.controller.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment

class DeploymentContext(private val deployment: V1FlinkDeployment) {
    // we should make copy of status to avoid side effects
    fun getDeploymentStatus() = deployment.status
}