package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.Result
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
    val controller: OperationController
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - Status.getOperatorTimestamp(flinkCluster).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - Status.getSavepointRequestTimestamp(flinkCluster).millis) / 1000L

    fun startCluster(clusterId: ClusterId, options: StartOptions) : Result<List<ClusterTask>> =
        controller.startCluster(clusterId, options, CacheAdapter(flinkCluster, resources))

    fun stopCluster(clusterId: ClusterId, options: StopOptions) : Result<List<ClusterTask>> =
        controller.stopCluster(clusterId, options, CacheAdapter(flinkCluster, resources))

    fun scaleCluster(clusterId: ClusterId, clusterScaling: ClusterScaling) : Result<List<ClusterTask>> =
        controller.scaleCluster(clusterId, clusterScaling, CacheAdapter(flinkCluster, resources))

    fun getClusterStatus(clusterId: ClusterId) : Result<Map<String, String>> =
        controller.getClusterStatus(clusterId, CacheAdapter(flinkCluster, resources))

    fun createClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        controller.createClusterResources(clusterId, clusterResources)

    fun deleteClusterResources(clusterId: ClusterId) : Result<Void?> =
        controller.deleteClusterResources(clusterId)

    fun removeJar(clusterId: ClusterId) : Result<Void?> =
        controller.removeJar(clusterId)

    fun isJarReady(clusterId: ClusterId) : Result<Void?> =
        controller.isJarReady(clusterId)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : Result<SavepointRequest> =
        controller.triggerSavepoint(clusterId, options)

    fun getLatestSavepoint(clusterId: ClusterId, savepointRequest: SavepointRequest) : Result<String> =
        controller.getLatestSavepoint(clusterId, savepointRequest)

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

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): Result<SavepointRequest> =
        controller.cancelJob(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ClusterScaling): Result<Void?> =
        controller.isClusterReady(clusterId, options)

    fun isClusterRunning(clusterId: ClusterId): Result<Boolean> =
        controller.isClusterRunning(clusterId)

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
}
