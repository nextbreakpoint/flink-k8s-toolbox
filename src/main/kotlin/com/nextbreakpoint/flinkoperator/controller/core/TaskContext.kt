package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus
import io.kubernetes.client.models.V1Job

class TaskContext(
    val operatorTimestamp: Long,
    val actionTimestamp: Long,
    val clusterId: ClusterId,
    val flinkCluster: V1FlinkCluster,
    val resources: CachedResources,
    val controller: OperationController
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

    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - Status.getOperatorTimestamp(flinkCluster)) / 1000L

    fun timeSinceLastSavepointInSeconds() = (controller.currentTimeMillis() - Status.getSavepointTimestamp(flinkCluster)) / 1000L

//    fun requestStartCluster(clusterId: ClusterId, options: StartOptions) : Result<Void?> =
//        controller.requestStartCluster(clusterId, options)
//
//    fun requestStopCluster(clusterId: ClusterId, options: StopOptions) : Result<Void?> =
//        controller.requestStopCluster(clusterId, options)
//
//    fun requestScaleCluster(clusterId: ClusterId, options: ScaleOptions): Result<Void?> =
//        controller.requestScaleCluster(clusterId, options)

    fun startCluster(clusterId: ClusterId, options: StartOptions) : Result<List<ClusterTask>> =
        controller.startCluster(clusterId, options)

    fun stopCluster(clusterId: ClusterId, options: StopOptions) : Result<List<ClusterTask>> =
        controller.stopCluster(clusterId, options)

    fun scaleCluster(clusterId: ClusterId, clusterScaling: ClusterScaling) : Result<List<ClusterTask>> =
        controller.scaleCluster(clusterId, clusterScaling)

//    fun createSavepoint(clusterId: ClusterId) : Result<List<ClusterTask>> =
//        controller.createSavepoint(clusterId)

    fun getClusterStatus(clusterId: ClusterId) : Result<Map<String, String>> =
        controller.getClusterStatus(clusterId)

//    fun createFlinkCluster(clusterId: ClusterId, flinkCluster: V1FlinkCluster) : Result<Void?> =
//        controller.createFlinkCluster(clusterId, flinkCluster)
//
//    fun deleteFlinkCluster(clusterId: ClusterId) : Result<Void?> =
//        controller.deleteFlinkCluster(clusterId)
//
//    fun updateClusterStatus(clusterId: ClusterId) : Result<Void?> =
//        controller.updateClusterStatus(clusterId)

    fun createClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        controller.createClusterResources(clusterId, clusterResources)

    fun deleteClusterResources(clusterId: ClusterId) : Result<Void?> =
        controller.deleteClusterResources(clusterId)

    fun removeJar(clusterId: ClusterId) : Result<Void?> =
        controller.removeJar(clusterId)

    fun isJarReady(clusterId: ClusterId) : Result<Void?> =
        controller.isJarReady(clusterId)

//    fun updateSavepoint(clusterId: ClusterId, savepointPath: String): Result<Void?> =
//        controller.updateSavepoint(clusterId, savepointPath)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : Result<SavepointRequest?> =
        controller.triggerSavepoint(clusterId, options)

    fun getSavepointStatus(clusterId: ClusterId, savepointRequest: SavepointRequest) : Result<String> =
        controller.getSavepointStatus(clusterId, savepointRequest)

    fun createBootstrapJob(clusterId: ClusterId, bootstrapJob: V1Job): Result<Void?> =
        controller.createBootstrapJob(clusterId, bootstrapJob)

    fun deleteBootstrapJob(clusterId: ClusterId) : Result<Void?> =
        controller.deleteBootstrapJob(clusterId)

    fun terminatePods(clusterId: ClusterId) : Result<Void?> =
        controller.terminatePods(clusterId)

    fun restartPods(clusterId: ClusterId, clusterResources: ClusterResources): Result<Void?> =
        controller.restartPods(clusterId, clusterResources)

    fun arePodsTerminated(clusterId: ClusterId): Result<Void?> =
        controller.arePodsTerminated(clusterId)

    fun startJob(clusterId: ClusterId, cluster: V1FlinkCluster) : Result<Void?> =
        controller.startJob(clusterId, cluster)

    fun stopJob(clusterId: ClusterId): Result<Void?> =
        controller.stopJob(clusterId)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): Result<SavepointRequest?> =
        controller.cancelJob(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ClusterScaling): Result<Void?> =
        controller.isClusterReady(clusterId, options)

    fun isClusterRunning(clusterId: ClusterId): Result<Boolean> =
        controller.isClusterRunning(clusterId)

    fun isClusterSuspended(clusterId: ClusterId): Result<Void?> =
        controller.isClusterSuspended(clusterId)

    fun isClusterTerminated(clusterId: ClusterId): Result<Void?> =
        controller.isClusterTerminated(clusterId)

    fun isJobStarted(clusterId: ClusterId): Result<Void?> =
        controller.isJobStarted(clusterId)

    fun isJobStopped(clusterId: ClusterId): Result<Void?> =
        controller.isJobStopped(clusterId)

    fun isJobRunning(clusterId: ClusterId): Result<Void?> =
        controller.isJobRunning(clusterId)

    fun setTaskManagersReplicas(clusterId: ClusterId, taskManagers: Int) : Result<Void?> =
        controller.setTaskManagersReplicas(clusterId, taskManagers)

    fun getTaskManagersReplicas(clusterId: ClusterId) : Result<Int> =
        controller.getTaskManagersReplicas(clusterId)

//    fun updateStatus(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
//        controller.updateStatus(clusterId, flinkCluster)
//    }
//
//    fun updateAnnotations(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
//        controller.updateAnnotations(clusterId, flinkCluster)
//    }
}
