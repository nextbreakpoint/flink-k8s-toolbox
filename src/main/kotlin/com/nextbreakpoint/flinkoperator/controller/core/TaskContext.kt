package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import io.kubernetes.client.models.V1Job

class TaskContext(
    val clusterId: ClusterId,
    val flinkCluster: V1FlinkCluster,
    val resources: CachedResources,
    private val controller: OperationController
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - Status.getOperatorTimestamp(flinkCluster).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - Status.getSavepointRequestTimestamp(flinkCluster).millis) / 1000L

    fun startCluster(clusterId: ClusterId, options: StartOptions) : OperationResult<List<ClusterTask>> =
        controller.startCluster(clusterId, options, CacheAdapter(flinkCluster, resources))

    fun stopCluster(clusterId: ClusterId, options: StopOptions) : OperationResult<List<ClusterTask>> =
        controller.stopCluster(clusterId, options, CacheAdapter(flinkCluster, resources))

    fun scaleCluster(clusterId: ClusterId, clusterScaling: ClusterScaling) : OperationResult<List<ClusterTask>> =
        controller.scaleCluster(clusterId, clusterScaling, CacheAdapter(flinkCluster, resources))

    fun createClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : OperationResult<Void?> =
        controller.createClusterResources(clusterId, clusterResources)

    fun deleteClusterResources(clusterId: ClusterId) : OperationResult<Void?> =
        controller.deleteClusterResources(clusterId)

    fun removeJar(clusterId: ClusterId) : OperationResult<Void?> =
        controller.removeJar(clusterId)

    fun isJarReady(clusterId: ClusterId) : OperationResult<Void?> =
        controller.isJarReady(clusterId)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : OperationResult<SavepointRequest> =
        controller.triggerSavepoint(clusterId, options)

    fun getLatestSavepoint(clusterId: ClusterId, savepointRequest: SavepointRequest) : OperationResult<String> =
        controller.getLatestSavepoint(clusterId, savepointRequest)

    fun createBootstrapJob(clusterId: ClusterId, bootstrapJob: V1Job): OperationResult<Void?> =
        controller.createBootstrapJob(clusterId, bootstrapJob)

    fun deleteBootstrapJob(clusterId: ClusterId) : OperationResult<Void?> =
        controller.deleteBootstrapJob(clusterId)

    fun terminatePods(clusterId: ClusterId) : OperationResult<Void?> =
        controller.terminatePods(clusterId)

    fun restartPods(clusterId: ClusterId, options: ClusterScaling): OperationResult<Void?> =
        controller.restartPods(clusterId, options)

    fun arePodsTerminated(clusterId: ClusterId): OperationResult<Void?> =
        controller.arePodsTerminated(clusterId)

    fun startJob(clusterId: ClusterId, cluster: V1FlinkCluster) : OperationResult<Void?> =
        controller.startJob(clusterId, cluster)

    fun stopJob(clusterId: ClusterId): OperationResult<Void?> =
        controller.stopJob(clusterId)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): OperationResult<SavepointRequest> =
        controller.cancelJob(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ClusterScaling): OperationResult<Void?> =
        controller.isClusterReady(clusterId, options)

    fun isClusterRunning(clusterId: ClusterId): OperationResult<Boolean> =
        controller.isClusterRunning(clusterId)

    fun isClusterTerminated(clusterId: ClusterId): OperationResult<Void?> =
        controller.isClusterTerminated(clusterId)

    fun isJobStarted(clusterId: ClusterId): OperationResult<Void?> =
        controller.isJobStarted(clusterId)

    fun isJobStopped(clusterId: ClusterId): OperationResult<Void?> =
        controller.isJobStopped(clusterId)

    fun isJobRunning(clusterId: ClusterId): OperationResult<Void?> =
        controller.isJobRunning(clusterId)

    fun isJobFinished(clusterId: ClusterId): OperationResult<Void?> =
        controller.isJobFinished(clusterId)

    fun setTaskManagersReplicas(clusterId: ClusterId, taskManagers: Int) : OperationResult<Void?> =
        controller.setTaskManagersReplicas(clusterId, taskManagers)

    fun getTaskManagersReplicas(clusterId: ClusterId) : OperationResult<Int> =
        controller.getTaskManagersReplicas(clusterId)
}
