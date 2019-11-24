package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus

data class ClusterResourcesStatus(
    val jobmanagerService: Pair<ResourceStatus, List<String>>,
    val jobmanagerStatefulSet: Pair<ResourceStatus, List<String>>,
    val taskmanagerStatefulSet: Pair<ResourceStatus, List<String>>
)

