package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import io.kubernetes.client.models.V1Job

interface BootstrapJobFactory {
    fun createBootstrapJob(
        clusterId: ClusterId,
        clusterOwner: String,
        bootstrap: V1BootstrapSpec
    ): V1Job
}