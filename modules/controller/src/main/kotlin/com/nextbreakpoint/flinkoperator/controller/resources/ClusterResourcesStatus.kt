package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus

data class ClusterResourcesStatus(
    val jarUploadJob: Pair<ResourceStatus, List<String>>,
    val jobmanagerService: Pair<ResourceStatus, List<String>>,
    val jobmanagerStatefulSet: Pair<ResourceStatus, List<String>>,
    val taskmanagerStatefulSet: Pair<ResourceStatus, List<String>>
)

