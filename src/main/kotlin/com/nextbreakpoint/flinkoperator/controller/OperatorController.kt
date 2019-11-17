package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCreateResources
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeleteResources
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterGetStatus
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsReady
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsRunning
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsSuspended
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsTerminated
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterStart
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterStop
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterScale
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointUpdate
import com.nextbreakpoint.flinkoperator.controller.operation.UpdateStatus
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterCreate
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterDelete
import com.nextbreakpoint.flinkoperator.controller.operation.JarIsReady
import com.nextbreakpoint.flinkoperator.controller.operation.JarRemove
import com.nextbreakpoint.flinkoperator.controller.operation.JobStart
import com.nextbreakpoint.flinkoperator.controller.operation.CreateBootstrapJob
import com.nextbreakpoint.flinkoperator.controller.operation.JobCancel
import com.nextbreakpoint.flinkoperator.controller.operation.JobHasStarted
import com.nextbreakpoint.flinkoperator.controller.operation.JobHasStopped
import com.nextbreakpoint.flinkoperator.controller.operation.JobStop
import com.nextbreakpoint.flinkoperator.controller.operation.PodsAreTerminated
import com.nextbreakpoint.flinkoperator.controller.operation.PodsRestart
import com.nextbreakpoint.flinkoperator.controller.operation.PodsTerminate
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterScale
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStart
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStop
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCheckpointing
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterReplaceResources
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointGetStatus
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointTrigger
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagersGetReplicas
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagersSetReplicas
import com.nextbreakpoint.flinkoperator.controller.operation.DeleteBootstrapJob
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources

class OperatorController(
    val flinkOptions: FlinkOptions,
    val flinkContext: FlinkContext,
    val kubernetesContext: KubernetesContext,
    val cache: OperatorCache,
    val taskHandlers: Map<OperatorTask, OperatorTaskHandler>
) {
    fun requestStartCluster(clusterId: ClusterId, options: StartOptions) : Result<Void?> =
        RequestClusterStart(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, options)

    fun requestStopCluster(clusterId: ClusterId, options: StopOptions) : Result<Void?> =
        RequestClusterStop(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, options)

    fun requestScaleCluster(clusterId: ClusterId, options: ScaleOptions): Result<Void?> =
        RequestClusterScale(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, options)

    fun startCluster(clusterId: ClusterId, options: StartOptions) : Result<List<OperatorTask>> =
        ClusterStart(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, options)

    fun stopCluster(clusterId: ClusterId, options: StopOptions) : Result<List<OperatorTask>> =
        ClusterStop(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, options)

    fun scaleCluster(clusterId: ClusterId, options: ScaleOptions) : Result<List<OperatorTask>> =
        ClusterScale(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, options)

    fun createSavepoint(clusterId: ClusterId) : Result<List<OperatorTask>> =
        ClusterCheckpointing(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, null)

    fun getClusterStatus(clusterId: ClusterId) : Result<Map<String, String>> =
        ClusterGetStatus(flinkOptions, flinkContext, kubernetesContext, cache).execute(clusterId, null)

    fun createFlinkCluster(clusterId: ClusterId, flinkCluster: V1FlinkCluster) : Result<Void?> =
        FlinkClusterCreate(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, flinkCluster)

    fun deleteFlinkCluster(clusterId: ClusterId) : Result<Void?> =
        FlinkClusterDelete(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun updateClusterStatus(clusterId: ClusterId) : Result<Void?> =
        UpdateStatus(this).execute(clusterId, null)

    fun createClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        ClusterCreateResources(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, clusterResources)

    fun deleteClusterResources(clusterId: ClusterId) : Result<Void?> =
        ClusterDeleteResources(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun replaceClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        ClusterReplaceResources(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, clusterResources)

    fun removeJar(clusterId: ClusterId) : Result<Void?> =
        JarRemove(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun isJarReady(clusterId: ClusterId) : Result<Void?> =
        JarIsReady(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun updateSavepoint(clusterId: ClusterId, savepointPath: String): Result<Void?> =
        SavepointUpdate(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, savepointPath)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : Result<SavepointRequest?> =
        SavepointTrigger(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, options)

    fun getSavepointStatus(clusterId: ClusterId, savepointRequest: SavepointRequest) : Result<String> =
        SavepointGetStatus(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, savepointRequest)

    fun createBootstrapJob(clusterId: ClusterId, clusterResources: ClusterResources): Result<Void?> =
        CreateBootstrapJob(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, clusterResources)

    fun deleteBootstrapJob(clusterId: ClusterId) : Result<Void?> =
        DeleteBootstrapJob(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun terminatePods(clusterId: ClusterId) : Result<Void?> =
        PodsTerminate(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun restartPods(clusterId: ClusterId, clusterResources: ClusterResources): Result<Void?> =
        PodsRestart(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, clusterResources)

    fun arePodsTerminated(clusterId: ClusterId): Result<Void?> =
        PodsAreTerminated(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun startJob(clusterId: ClusterId, cluster: V1FlinkCluster) : Result<Void?> =
        JobStart(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, cluster)

    fun stopJob(clusterId: ClusterId): Result<Void?> =
        JobStop(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): Result<SavepointRequest?> =
        JobCancel(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ScaleOptions): Result<Void?> =
        ClusterIsReady(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, options)

    fun isClusterRunning(clusterId: ClusterId): Result<Boolean> =
        ClusterIsRunning(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun isClusterSuspended(clusterId: ClusterId): Result<Void?> =
        ClusterIsSuspended(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun isClusterTerminated(clusterId: ClusterId): Result<Void?> =
        ClusterIsTerminated(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun isJobStarted(clusterId: ClusterId): Result<Void?> =
        JobHasStarted(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun isJobStopped(clusterId: ClusterId): Result<Void?> =
        JobHasStopped(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun setTaskManagersReplicas(clusterId: ClusterId, taskManagers: Int) : Result<Void?> =
        TaskManagersSetReplicas(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, taskManagers)

    fun getTaskManagersReplicas(clusterId: ClusterId) : Result<Int> =
        TaskManagersGetReplicas(flinkOptions, flinkContext, kubernetesContext).execute(clusterId, null)

    fun updateState(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubernetesContext.updateStatus(clusterId, flinkCluster.status)
    }

    fun updateAnnotations(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubernetesContext.updateAnnotations(clusterId, flinkCluster.metadata.annotations)
    }

    fun currentTimeMillis() = System.currentTimeMillis()
}