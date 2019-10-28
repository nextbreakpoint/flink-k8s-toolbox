package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus

class OperatorContext(
    val lastUpdated: Long,
    val clusterId: ClusterId,
    val flinkCluster: V1FlinkCluster,
    val controller: OperatorController,
    val resources: OperatorResources
) {
    fun haveClusterResourcesDiverged(clusterResourcesStatus: ClusterResourcesStatus): Boolean {
        if (clusterResourcesStatus.jobmanagerService.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterResourcesStatus.jobmanagerStatefulSet.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterResourcesStatus.taskmanagerStatefulSet.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }

    fun haveUploadJobResourceDiverged(clusterResourcesStatus: ClusterResourcesStatus): Boolean {
        if (clusterResourcesStatus.jarUploadJob.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }
}
