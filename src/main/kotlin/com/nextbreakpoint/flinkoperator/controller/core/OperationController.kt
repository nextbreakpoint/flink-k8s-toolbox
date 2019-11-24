package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.operation.BootstrapCreateJob
import com.nextbreakpoint.flinkoperator.controller.operation.BootstrapDeleteJob
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCreateResources
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeleteResources
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterGetStatus
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsReady
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsRunning
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsSuspended
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsTerminated
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterScale
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterStart
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterStop
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterCreate
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterDelete
import com.nextbreakpoint.flinkoperator.controller.operation.JarIsReady
import com.nextbreakpoint.flinkoperator.controller.operation.JarRemove
import com.nextbreakpoint.flinkoperator.controller.operation.JobCancel
import com.nextbreakpoint.flinkoperator.controller.operation.JobHasStarted
import com.nextbreakpoint.flinkoperator.controller.operation.JobHasStopped
import com.nextbreakpoint.flinkoperator.controller.operation.JobIsRunning
import com.nextbreakpoint.flinkoperator.controller.operation.JobStart
import com.nextbreakpoint.flinkoperator.controller.operation.JobStop
import com.nextbreakpoint.flinkoperator.controller.operation.PodsAreTerminated
import com.nextbreakpoint.flinkoperator.controller.operation.PodsScaleDown
import com.nextbreakpoint.flinkoperator.controller.operation.PodsScaleUp
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterScale
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStart
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStop
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointCreate
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointForget
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointGetStatus
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointTrigger
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagersGetReplicas
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagersSetReplicas
import com.nextbreakpoint.flinkoperator.controller.operation.UpdateClusterStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import io.kubernetes.client.models.V1Job

class OperationController(
    val flinkOptions: FlinkOptions,
    val flinkClient: FlinkClient,
    val kubeClient: KubeClient,
    val cache: Cache,
    val taskHandlers: Map<ClusterTask, Task>
) {
    fun requestStartCluster(clusterId: ClusterId, options: StartOptions) : Result<Void?> =
        RequestClusterStart(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, options)

    fun requestStopCluster(clusterId: ClusterId, options: StopOptions) : Result<Void?> =
        RequestClusterStop(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, options)

    fun requestScaleCluster(clusterId: ClusterId, options: ScaleOptions): Result<Void?> =
        RequestClusterScale(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun startCluster(clusterId: ClusterId, options: StartOptions) : Result<List<ClusterTask>> =
        ClusterStart(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, options)

    fun stopCluster(clusterId: ClusterId, options: StopOptions) : Result<List<ClusterTask>> =
        ClusterStop(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, options)

    fun scaleCluster(clusterId: ClusterId, clusterScaling: ClusterScaling) : Result<List<ClusterTask>> =
        ClusterScale(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, clusterScaling)

    fun createSavepoint(clusterId: ClusterId) : Result<List<ClusterTask>> =
        SavepointCreate(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, null)

    fun forgetSavepoint(clusterId: ClusterId) : Result<List<ClusterTask>> =
        SavepointForget(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, null)

    fun getClusterStatus(clusterId: ClusterId) : Result<Map<String, String>> =
        ClusterGetStatus(flinkOptions, flinkClient, kubeClient, cache).execute(clusterId, null)

    fun createFlinkCluster(clusterId: ClusterId, flinkCluster: V1FlinkCluster) : Result<Void?> =
        FlinkClusterCreate(flinkOptions, flinkClient, kubeClient).execute(clusterId, flinkCluster)

    fun deleteFlinkCluster(clusterId: ClusterId) : Result<Void?> =
        FlinkClusterDelete(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun updateClusterStatus(clusterId: ClusterId) : Result<Void?> =
        UpdateClusterStatus(this).execute(clusterId, null)

    fun createClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        ClusterCreateResources(flinkOptions, flinkClient, kubeClient).execute(clusterId, clusterResources)

    fun deleteClusterResources(clusterId: ClusterId) : Result<Void?> =
        ClusterDeleteResources(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun removeJar(clusterId: ClusterId) : Result<Void?> =
        JarRemove(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isJarReady(clusterId: ClusterId) : Result<Void?> =
        JarIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : Result<SavepointRequest?> =
        SavepointTrigger(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun getSavepointStatus(clusterId: ClusterId, savepointRequest: SavepointRequest) : Result<String> =
        SavepointGetStatus(flinkOptions, flinkClient, kubeClient).execute(clusterId, savepointRequest)

    fun createBootstrapJob(clusterId: ClusterId, bootstrapJob: V1Job): Result<Void?> =
        BootstrapCreateJob(flinkOptions, flinkClient, kubeClient).execute(clusterId, bootstrapJob)

    fun deleteBootstrapJob(clusterId: ClusterId) : Result<Void?> =
        BootstrapDeleteJob(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun terminatePods(clusterId: ClusterId) : Result<Void?> =
        PodsScaleDown(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun restartPods(clusterId: ClusterId, clusterResources: ClusterResources): Result<Void?> =
        PodsScaleUp(flinkOptions, flinkClient, kubeClient).execute(clusterId, clusterResources)

    fun arePodsTerminated(clusterId: ClusterId): Result<Void?> =
        PodsAreTerminated(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun startJob(clusterId: ClusterId, cluster: V1FlinkCluster) : Result<Void?> =
        JobStart(flinkOptions, flinkClient, kubeClient).execute(clusterId, cluster)

    fun stopJob(clusterId: ClusterId): Result<Void?> =
        JobStop(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): Result<SavepointRequest?> =
        JobCancel(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ClusterScaling): Result<Void?> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun isClusterRunning(clusterId: ClusterId): Result<Boolean> =
        ClusterIsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isClusterSuspended(clusterId: ClusterId): Result<Void?> =
        ClusterIsSuspended(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isClusterTerminated(clusterId: ClusterId): Result<Void?> =
        ClusterIsTerminated(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isJobStarted(clusterId: ClusterId): Result<Void?> =
        JobHasStarted(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isJobStopped(clusterId: ClusterId): Result<Void?> =
        JobHasStopped(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isJobRunning(clusterId: ClusterId): Result<Void?> =
        JobIsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun setTaskManagersReplicas(clusterId: ClusterId, taskManagers: Int) : Result<Void?> =
        TaskManagersSetReplicas(flinkOptions, flinkClient, kubeClient).execute(clusterId, taskManagers)

    fun getTaskManagersReplicas(clusterId: ClusterId) : Result<Int> =
        TaskManagersGetReplicas(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun updateStatus(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubeClient.updateStatus(clusterId, flinkCluster.status)
    }

    fun updateAnnotations(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubeClient.updateAnnotations(clusterId, flinkCluster.metadata.annotations)
    }

    fun currentTimeMillis() = System.currentTimeMillis()
}