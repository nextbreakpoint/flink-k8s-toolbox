package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus

data class ClusterResourcesStatus(
    val service: Pair<ResourceStatus, List<String>>,
    val jobmanagerPod: Pair<ResourceStatus, List<String>>,
    val taskmanagerPod: Pair<ResourceStatus, List<String>>
)

