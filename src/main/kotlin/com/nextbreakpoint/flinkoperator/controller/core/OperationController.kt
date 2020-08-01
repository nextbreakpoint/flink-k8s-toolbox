package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterScale
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.PodReplicas
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.operation.ArePodsRunning
import com.nextbreakpoint.flinkoperator.controller.operation.ArePodsTerminated
import com.nextbreakpoint.flinkoperator.controller.operation.BootstrapCreateJob
import com.nextbreakpoint.flinkoperator.controller.operation.BootstrapDeleteJob
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCreateService
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeleteService
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterGetStatus
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterIsReady
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterCreate
import com.nextbreakpoint.flinkoperator.controller.operation.FlinkClusterDelete
import com.nextbreakpoint.flinkoperator.controller.operation.JarRemove
import com.nextbreakpoint.flinkoperator.controller.operation.JobCancel
import com.nextbreakpoint.flinkoperator.controller.operation.JobIsFailed
import com.nextbreakpoint.flinkoperator.controller.operation.JobIsFinished
import com.nextbreakpoint.flinkoperator.controller.operation.JobIsRunning
import com.nextbreakpoint.flinkoperator.controller.operation.JobStart
import com.nextbreakpoint.flinkoperator.controller.operation.JobStop
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterDeletePods
import com.nextbreakpoint.flinkoperator.controller.operation.ClusterCreatePods
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterScale
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStart
import com.nextbreakpoint.flinkoperator.controller.operation.RequestClusterStop
import com.nextbreakpoint.flinkoperator.controller.operation.RequestSavepointForget
import com.nextbreakpoint.flinkoperator.controller.operation.RequestSavepointTrigger
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointQuery
import com.nextbreakpoint.flinkoperator.controller.operation.SavepointTrigger
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service

class OperationController(
    private val flinkOptions: FlinkOptions,
    private val flinkClient: FlinkClient,
    private val kubeClient: KubeClient
) {
    fun currentTimeMillis() = System.currentTimeMillis()

    fun requestScaleCluster(clusterSelector: ClusterSelector, options: ScaleOptions): OperationResult<Void?> =
        RequestClusterScale(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun requestStartCluster(clusterSelector: ClusterSelector, options: StartOptions, bridge: CacheBridge) : OperationResult<Void?> =
        RequestClusterStart(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterSelector, options)

    fun requestStopCluster(clusterSelector: ClusterSelector, options: StopOptions, bridge: CacheBridge) : OperationResult<Void?> =
        RequestClusterStop(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterSelector, options)

    fun createSavepoint(clusterSelector: ClusterSelector, bridge: CacheBridge) : OperationResult<Void?> =
        RequestSavepointTrigger(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterSelector, null)

    fun forgetSavepoint(clusterSelector: ClusterSelector, bridge: CacheBridge) : OperationResult<Void?> =
        RequestSavepointForget(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterSelector, null)

    fun getClusterStatus(clusterSelector: ClusterSelector, bridge: CacheBridge) : OperationResult<String> =
        ClusterGetStatus(flinkOptions, flinkClient, kubeClient, bridge).execute(clusterSelector, null)

    fun createFlinkCluster(clusterSelector: ClusterSelector, flinkCluster: V1FlinkCluster) : OperationResult<Void?> =
        FlinkClusterCreate(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, flinkCluster)

    fun deleteFlinkCluster(clusterSelector: ClusterSelector) : OperationResult<Void?> =
        FlinkClusterDelete(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createService(clusterSelector: ClusterSelector, service: V1Service): OperationResult<String?> =
        ClusterCreateService(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, service)

    fun deleteService(clusterSelector: ClusterSelector): OperationResult<Void?> =
        ClusterDeleteService(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun removeJar(clusterSelector: ClusterSelector) : OperationResult<Void?> =
        JarRemove(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun triggerSavepoint(clusterSelector: ClusterSelector, options: SavepointOptions) : OperationResult<SavepointRequest?> =
        SavepointTrigger(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun querySavepoint(clusterSelector: ClusterSelector, savepointRequest: SavepointRequest) : OperationResult<String?> =
        SavepointQuery(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, savepointRequest)

    fun createBootstrapJob(clusterSelector: ClusterSelector, bootstrapJob: V1Job): OperationResult<String?> =
        BootstrapCreateJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, bootstrapJob)

    fun deleteBootstrapJob(clusterSelector: ClusterSelector) : OperationResult<Void?> =
        BootstrapDeleteJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createPods(clusterSelector: ClusterSelector, options: PodReplicas): OperationResult<Set<String>> =
        ClusterCreatePods(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun deletePods(clusterSelector: ClusterSelector, options: DeleteOptions) : OperationResult<Void?> =
        ClusterDeletePods(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun arePodsRunning(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        ArePodsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun arePodsTerminated(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        ArePodsTerminated(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun startJob(clusterSelector: ClusterSelector, cluster: V1FlinkCluster) : OperationResult<Void?> =
        JobStart(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, cluster)

    fun stopJob(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        JobStop(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun cancelJob(clusterSelector: ClusterSelector, options: SavepointOptions): OperationResult<SavepointRequest?> =
        JobCancel(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun isClusterReady(clusterSelector: ClusterSelector, options: ClusterScale): OperationResult<Boolean> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun isJobFinished(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        JobIsFinished(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun isJobRunning(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        JobIsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun isJobFailed(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        JobIsFailed(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun updateStatus(clusterSelector: ClusterSelector, flinkCluster: V1FlinkCluster) {
        kubeClient.updateStatus(clusterSelector, flinkCluster.status)
    }

    fun updateAnnotations(clusterSelector: ClusterSelector, flinkCluster: V1FlinkCluster) {
        kubeClient.updateAnnotations(clusterSelector, flinkCluster.metadata.annotations)
    }

    fun updateFinalizers(clusterSelector: ClusterSelector, flinkCluster: V1FlinkCluster) {
        kubeClient.updateFinalizers(clusterSelector, flinkCluster.metadata.finalizers)
    }
}