package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import io.kubernetes.client.models.V1Job

interface BootstrapResourcesFactory {
    fun createBootstrapJob(
        clusterSelector: ClusterSelector,
        clusterOwner: String,
        bootstrap: V1BootstrapSpec,
        savepointPath: String?,
        parallelism: Int
    ): V1Job
}