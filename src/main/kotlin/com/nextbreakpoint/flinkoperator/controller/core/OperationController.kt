package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.operation.BootstrapCreateJob
import com.nextbreakpoint.flinkoperator.controller.operation.BootstrapDeleteJob
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCreateService
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCreateStatefulSet
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeletePVCs
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeleteService
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeleteStatefulSets
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterGetStatus
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsReady
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterCreate
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterDelete
import com.nextbreakpoint.flinkoperator.controller.operation.JarRemove
import com.nextbreakpoint.flinkoperator.controller.operation.JobCancel
import com.nextbreakpoint.flinkoperator.controller.operation.JobIsFinished
import com.nextbreakpoint.flinkoperator.controller.operation.JobIsRunning
import com.nextbreakpoint.flinkoperator.controller.operation.JobStart
import com.nextbreakpoint.flinkoperator.controller.operation.JobStop
import com.nextbreakpoint.flinkoperator.controller.operation.PodsAreTerminated
import com.nextbreakpoint.flinkoperator.controller.operation.PodsScaleDown
import com.nextbreakpoint.flinkoperator.controller.operation.PodsScaleUp
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterScale
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStart
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStop
import com.nextbreakpoint.flinkoperator.controller.operation.RequestSavepointForget
import com.nextbreakpoint.flinkoperator.controller.operation.RequestSavepointTrigger
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointGetLatest
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointTrigger
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

class OperationController(
    private val flinkOptions: FlinkOptions,
    private val flinkClient: FlinkClient,
    private val kubeClient: KubeClient
) {
    fun currentTimeMillis() = System.currentTimeMillis()

    fun requestScaleCluster(clusterId: ClusterId, options: ScaleOptions): OperationResult<Void?> =
        RequestClusterScale(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun requestStartCluster(clusterId: ClusterId, options: StartOptions, bridge: CacheBridge) : OperationResult<Void?> =
        RequestClusterStart(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterId, options)

    fun requestStopCluster(clusterId: ClusterId, options: StopOptions, bridge: CacheBridge) : OperationResult<Void?> =
        RequestClusterStop(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterId, options)

    fun createSavepoint(clusterId: ClusterId, bridge: CacheBridge) : OperationResult<Void?> =
        RequestSavepointTrigger(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterId, null)

    fun forgetSavepoint(clusterId: ClusterId, bridge: CacheBridge) : OperationResult<Void?> =
        RequestSavepointForget(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterId, null)

    fun getClusterStatus(clusterId: ClusterId, bridge: CacheBridge) : OperationResult<String> =
        ClusterGetStatus(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterId, null)

    fun createFlinkCluster(clusterId: ClusterId, flinkCluster: V1FlinkCluster) : OperationResult<Void?> =
        FlinkClusterCreate(flinkOptions, flinkClient, kubeClient).execute(clusterId, flinkCluster)

    fun deleteFlinkCluster(clusterId: ClusterId) : OperationResult<Void?> =
        FlinkClusterDelete(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun createJobManagerService(clusterId: ClusterId, service: V1Service): OperationResult<String?> =
        ClusterCreateService(flinkOptions, flinkClient, kubeClient).execute(clusterId, service)

    fun deleteJobManagerService(clusterId: ClusterId): OperationResult<Void?> =
        ClusterDeleteService(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun createStatefulSet(clusterId: ClusterId, statefulSet: V1StatefulSet): OperationResult<String?> =
        ClusterCreateStatefulSet(flinkOptions, flinkClient, kubeClient).execute(clusterId, statefulSet)

    fun deleteStatefulSets(clusterId: ClusterId): OperationResult<Void?> =
        ClusterDeleteStatefulSets(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun deletePersistentVolumeClaims(clusterId: ClusterId): OperationResult<Void?> =
        ClusterDeletePVCs(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun removeJar(clusterId: ClusterId) : OperationResult<Void?> =
        JarRemove(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : OperationResult<SavepointRequest> =
        SavepointTrigger(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun getLatestSavepoint(clusterId: ClusterId, savepointRequest: SavepointRequest) : OperationResult<String> =
        SavepointGetLatest(flinkOptions, flinkClient, kubeClient).execute(clusterId, savepointRequest)

    fun createBootstrapJob(clusterId: ClusterId, bootstrapJob: V1Job): OperationResult<String?> =
        BootstrapCreateJob(flinkOptions, flinkClient, kubeClient).execute(clusterId, bootstrapJob)

    fun deleteBootstrapJob(clusterId: ClusterId) : OperationResult<Void?> =
        BootstrapDeleteJob(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun terminatePods(clusterId: ClusterId) : OperationResult<Void?> =
        PodsScaleDown(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun restartPods(clusterId: ClusterId, options: ClusterScaling): OperationResult<Void?> =
        PodsScaleUp(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun arePodsTerminated(clusterId: ClusterId): OperationResult<Void?> =
        PodsAreTerminated(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun startJob(clusterId: ClusterId, cluster: V1FlinkCluster) : OperationResult<Void?> =
        JobStart(flinkOptions, flinkClient, kubeClient).execute(clusterId, cluster)

    fun stopJob(clusterId: ClusterId): OperationResult<Void?> =
        JobStop(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): OperationResult<SavepointRequest> =
        JobCancel(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ClusterScaling): OperationResult<Void?> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterId, options)

    fun isJobRunning(clusterId: ClusterId): OperationResult<Void?> =
        JobIsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun isJobFinished(clusterId: ClusterId): OperationResult<Void?> =
        JobIsFinished(flinkOptions, flinkClient, kubeClient).execute(clusterId, null)

    fun updateStatus(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubeClient.updateStatus(clusterId, flinkCluster.status)
    }

    fun updateAnnotations(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubeClient.updateAnnotations(clusterId, flinkCluster.metadata.annotations)
    }

    fun updateFinalizers(clusterId: ClusterId, flinkCluster: V1FlinkCluster) {
        kubeClient.updateFinalizers(clusterId, flinkCluster.metadata.finalizers)
    }
}