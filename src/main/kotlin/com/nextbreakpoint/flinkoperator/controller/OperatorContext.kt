package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus

class OperatorContext(
    val operatorTimestamp: Long,
    val actionTimestamp: Long,
    val clusterId: ClusterId,
    val flinkCluster: V1FlinkCluster,
    val resources: OperatorResources,
    val controller: OperatorController
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

    fun hasBootstrapJobDiverged(clusterResourcesStatus: ClusterResourcesStatus): Boolean {
        if (clusterResourcesStatus.bootstrapJob.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }
}
