package com.nextbreakpoint.flinkoperator.server.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.ClusterScale
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.PodReplicas
import com.nextbreakpoint.flinkoperator.common.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.StartOptions
import com.nextbreakpoint.flinkoperator.common.StopOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.action.ArePodsRunning
import com.nextbreakpoint.flinkoperator.server.controller.action.ArePodsTerminated
import com.nextbreakpoint.flinkoperator.server.controller.action.BootstrapCreateJob
import com.nextbreakpoint.flinkoperator.server.controller.action.BootstrapDeleteJob
import com.nextbreakpoint.flinkoperator.server.controller.action.ClusterCreateService
import com.nextbreakpoint.flinkoperator.server.controller.action.ClusterDeleteService
import com.nextbreakpoint.flinkoperator.server.controller.action.ClusterGetStatus
import com.nextbreakpoint.flinkoperator.server.controller.action.ClusterIsReady
import com.nextbreakpoint.flinkoperator.server.controller.action.FlinkClusterCreate
import com.nextbreakpoint.flinkoperator.server.controller.action.FlinkClusterDelete
import com.nextbreakpoint.flinkoperator.server.controller.action.JarRemove
import com.nextbreakpoint.flinkoperator.server.controller.action.JobCancel
import com.nextbreakpoint.flinkoperator.server.controller.action.JobIsFailed
import com.nextbreakpoint.flinkoperator.server.controller.action.JobIsFinished
import com.nextbreakpoint.flinkoperator.server.controller.action.JobIsRunning
import com.nextbreakpoint.flinkoperator.server.controller.action.JobStart
import com.nextbreakpoint.flinkoperator.server.controller.action.JobStop
import com.nextbreakpoint.flinkoperator.server.controller.action.ClusterDeletePods
import com.nextbreakpoint.flinkoperator.server.controller.action.ClusterCreatePods
import com.nextbreakpoint.flinkoperator.server.controller.action.RequestClusterScale
import com.nextbreakpoint.flinkoperator.server.controller.action.RequestClusterStart
import com.nextbreakpoint.flinkoperator.server.controller.action.RequestClusterStop
import com.nextbreakpoint.flinkoperator.server.controller.action.RequestSavepointForget
import com.nextbreakpoint.flinkoperator.server.controller.action.RequestSavepointTrigger
import com.nextbreakpoint.flinkoperator.server.controller.action.SavepointQuery
import com.nextbreakpoint.flinkoperator.server.controller.action.SavepointTrigger
import com.nextbreakpoint.flinkoperator.server.controller.action.SupervisorCreateDeployment
import com.nextbreakpoint.flinkoperator.server.controller.action.SupervisorDeleteDeployment
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service

class Controller(
    private val flinkOptions: FlinkOptions,
    private val flinkClient: FlinkClient,
    private val kubeClient: KubeClient
) {
    fun currentTimeMillis() = System.currentTimeMillis()

    fun requestScaleCluster(clusterSelector: ClusterSelector, options: ScaleOptions): Result<Void?> =
        RequestClusterScale(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun requestStartCluster(clusterSelector: ClusterSelector, options: StartOptions, context: ControllerContext) : Result<Void?> =
        RequestClusterStart(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, options)

    fun requestStopCluster(clusterSelector: ClusterSelector, options: StopOptions, context: ControllerContext) : Result<Void?> =
        RequestClusterStop(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, options)

    fun createSavepoint(clusterSelector: ClusterSelector, context: ControllerContext) : Result<Void?> =
        RequestSavepointTrigger(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun forgetSavepoint(clusterSelector: ClusterSelector, context: ControllerContext) : Result<Void?> =
        RequestSavepointForget(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun getClusterStatus(clusterSelector: ClusterSelector, context: ControllerContext) : Result<String> =
        ClusterGetStatus(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun createFlinkCluster(clusterSelector: ClusterSelector, flinkCluster: V1FlinkCluster) : Result<Void?> =
        FlinkClusterCreate(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, flinkCluster)

    fun deleteFlinkCluster(clusterSelector: ClusterSelector) : Result<Void?> =
        FlinkClusterDelete(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createService(clusterSelector: ClusterSelector, service: V1Service): Result<String?> =
        ClusterCreateService(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, service)

    fun deleteService(clusterSelector: ClusterSelector): Result<Void?> =
        ClusterDeleteService(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createSupervisorDeployment(clusterSelector: ClusterSelector, deployment: V1Deployment) : Result<String?> =
        SupervisorCreateDeployment(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, deployment)

    fun deleteSupervisorDeployment(clusterSelector: ClusterSelector) : Result<Void?> =
        SupervisorDeleteDeployment(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun removeJar(clusterSelector: ClusterSelector) : Result<Void?> =
        JarRemove(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun triggerSavepoint(clusterSelector: ClusterSelector, options: SavepointOptions) : Result<SavepointRequest?> =
        SavepointTrigger(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun querySavepoint(clusterSelector: ClusterSelector, savepointRequest: SavepointRequest) : Result<String?> =
        SavepointQuery(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, savepointRequest)

    fun createBootstrapJob(clusterSelector: ClusterSelector, bootstrapJob: V1Job): Result<String?> =
        BootstrapCreateJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, bootstrapJob)

    fun deleteBootstrapJob(clusterSelector: ClusterSelector) : Result<Void?> =
        BootstrapDeleteJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createPods(clusterSelector: ClusterSelector, options: PodReplicas): Result<Set<String>> =
        ClusterCreatePods(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun deletePods(clusterSelector: ClusterSelector, options: DeleteOptions) : Result<Void?> =
        ClusterDeletePods(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun arePodsRunning(clusterSelector: ClusterSelector): Result<Boolean> =
        ArePodsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun arePodsTerminated(clusterSelector: ClusterSelector): Result<Boolean> =
        ArePodsTerminated(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun startJob(clusterSelector: ClusterSelector, cluster: V1FlinkCluster) : Result<Void?> =
        JobStart(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, cluster)

    fun stopJob(clusterSelector: ClusterSelector): Result<Boolean> =
        JobStop(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun cancelJob(clusterSelector: ClusterSelector, options: SavepointOptions): Result<SavepointRequest?> =
        JobCancel(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun isClusterReady(clusterSelector: ClusterSelector, options: ClusterScale): Result<Boolean> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun isJobFinished(clusterSelector: ClusterSelector): Result<Boolean> =
        JobIsFinished(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun isJobRunning(clusterSelector: ClusterSelector): Result<Boolean> =
        JobIsRunning(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun isJobFailed(clusterSelector: ClusterSelector): Result<Boolean> =
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

    fun updateAnnotations(clusterSelector: ClusterSelector, deployment: V1Deployment) {
        kubeClient.updateAnnotations(clusterSelector, deployment.metadata.annotations)
    }
}