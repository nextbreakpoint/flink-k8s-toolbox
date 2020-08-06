package com.nextbreakpoint.flink.k8s.controller

import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.PodReplicas
import com.nextbreakpoint.flink.common.RunJarOptions
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.action.BootstrapCreateJob
import com.nextbreakpoint.flink.k8s.controller.action.BootstrapDeleteJob
import com.nextbreakpoint.flink.k8s.controller.action.ClusterCreateFlinkJob
import com.nextbreakpoint.flink.k8s.controller.action.ClusterCreateService
import com.nextbreakpoint.flink.k8s.controller.action.ClusterDeleteService
import com.nextbreakpoint.flink.k8s.controller.action.ClusterGetStatus
import com.nextbreakpoint.flink.k8s.controller.action.ClusterIsReady
import com.nextbreakpoint.flink.k8s.controller.action.FlinkClusterCreate
import com.nextbreakpoint.flink.k8s.controller.action.FlinkClusterDelete
import com.nextbreakpoint.flink.k8s.controller.action.ClusterRemoveJars
import com.nextbreakpoint.flink.k8s.controller.action.JobCancel
import com.nextbreakpoint.flink.k8s.controller.action.ClusterStopJobs
import com.nextbreakpoint.flink.k8s.controller.action.ClusterDeletePods
import com.nextbreakpoint.flink.k8s.controller.action.ClusterCreatePods
import com.nextbreakpoint.flink.k8s.controller.action.ClusterDeleteFlinkJob
import com.nextbreakpoint.flink.k8s.controller.action.ClusterGetJobStatus
import com.nextbreakpoint.flink.k8s.controller.action.ClusterListJars
import com.nextbreakpoint.flink.k8s.controller.action.ClusterRunJar
import com.nextbreakpoint.flink.k8s.controller.action.ClusterUploadJar
import com.nextbreakpoint.flink.k8s.controller.action.JobGetStatus
import com.nextbreakpoint.flink.k8s.controller.action.JobStop
import com.nextbreakpoint.flink.k8s.controller.action.PodCreate
import com.nextbreakpoint.flink.k8s.controller.action.PodDelete
import com.nextbreakpoint.flink.k8s.controller.action.RequestClusterScale
import com.nextbreakpoint.flink.k8s.controller.action.RequestClusterStart
import com.nextbreakpoint.flink.k8s.controller.action.RequestClusterStop
import com.nextbreakpoint.flink.k8s.controller.action.RequestJobScale
import com.nextbreakpoint.flink.k8s.controller.action.RequestJobStart
import com.nextbreakpoint.flink.k8s.controller.action.RequestJobStop
import com.nextbreakpoint.flink.k8s.controller.action.RequestSavepointForget
import com.nextbreakpoint.flink.k8s.controller.action.RequestSavepointTrigger
import com.nextbreakpoint.flink.k8s.controller.action.SavepointQuery
import com.nextbreakpoint.flink.k8s.controller.action.SavepointTrigger
import com.nextbreakpoint.flink.k8s.controller.action.SupervisorCreateDeployment
import com.nextbreakpoint.flink.k8s.controller.action.SupervisorDeleteDeployment
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagersStatus
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import java.io.File

class Controller(
    private val flinkOptions: FlinkOptions,
    private val flinkClient: FlinkClient,
    private val kubeClient: KubeClient,
    private val dryRun: Boolean
) {
    fun currentTimeMillis() = System.currentTimeMillis()

    fun requestScaleCluster(clusterSelector: ResourceSelector, options: ScaleClusterOptions): Result<Void?> =
        RequestClusterScale(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun requestStartCluster(clusterSelector: ResourceSelector, options: StartOptions, context: ClusterContext) : Result<Void?> =
        RequestClusterStart(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, options)

    fun requestStopCluster(clusterSelector: ResourceSelector, options: StopOptions, context: ClusterContext) : Result<Void?> =
        RequestClusterStop(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, options)

    fun requestScaleJob(jobSelector: ResourceSelector, options: ScaleJobOptions): Result<Void?> =
        RequestJobScale(flinkOptions, flinkClient, kubeClient).execute(jobSelector, options)

    fun requestStartJob(jobSelector: ResourceSelector, options: StartOptions, context: JobContext) : Result<Void?> =
        RequestJobStart(flinkOptions, flinkClient, kubeClient, context).execute(jobSelector, options)

    fun requestStopJob(jobSelector: ResourceSelector, options: StopOptions, context: JobContext) : Result<Void?> =
        RequestJobStop(flinkOptions, flinkClient, kubeClient, context).execute(jobSelector, options)

    fun requestTriggerSavepoint(clusterSelector: ResourceSelector, context: JobContext) : Result<Void?> =
        RequestSavepointTrigger(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun requestForgetSavepoint(clusterSelector: ResourceSelector, context: JobContext) : Result<Void?> =
        RequestSavepointForget(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun getClusterStatus(clusterSelector: ResourceSelector, context: ClusterContext) : Result<String> =
        ClusterGetStatus(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun createFlinkCluster(clusterSelector: ResourceSelector, flinkCluster: V2FlinkCluster) : Result<Void?> =
        FlinkClusterCreate(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, flinkCluster)

    fun deleteFlinkCluster(clusterSelector: ResourceSelector) : Result<Void?> =
        FlinkClusterDelete(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createService(clusterSelector: ResourceSelector, service: V1Service): Result<String?> =
        ClusterCreateService(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, service)

    fun deleteService(clusterSelector: ResourceSelector): Result<Void?> =
        ClusterDeleteService(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createSupervisorDeployment(clusterSelector: ResourceSelector, deployment: V1Deployment) : Result<String?> =
        SupervisorCreateDeployment(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, deployment)

    fun deleteSupervisorDeployment(clusterSelector: ResourceSelector) : Result<Void?> =
        SupervisorDeleteDeployment(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun removeJars(clusterSelector: ResourceSelector) : Result<Void?> =
        ClusterRemoveJars(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun listJars(clusterSelector: ResourceSelector) : Result<List<JarFileInfo>> =
        ClusterListJars(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun runJar(clusterSelector: ResourceSelector, options: RunJarOptions) : Result<String> =
        ClusterRunJar(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun uploadJar(clusterSelector: ResourceSelector, file: File): Result<JarUploadResponseBody?> =
        ClusterUploadJar(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, file)

    fun triggerSavepoint(clusterSelector: ResourceSelector, options: SavepointOptions, context: JobContext) : Result<SavepointRequest?> =
        SavepointTrigger(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, options)

    fun querySavepoint(clusterSelector: ResourceSelector, savepointRequest: SavepointRequest, context: JobContext) : Result<String?> =
        SavepointQuery(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, savepointRequest)

    fun createBootstrapJob(clusterSelector: ResourceSelector, bootstrapJob: V1Job): Result<String?> =
        BootstrapCreateJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, bootstrapJob)

    fun deleteBootstrapJob(clusterSelector: ResourceSelector, jobName: String) : Result<Void?> =
        BootstrapDeleteJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, jobName)

    fun createFlinkJob(clusterSelector: ResourceSelector, job: V1FlinkJob): Result<Void?> =
        ClusterCreateFlinkJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, job)

    fun deleteFlinkJob(clusterSelector: ResourceSelector, jobName: String) : Result<Void?> =
        ClusterDeleteFlinkJob(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, jobName)

    fun createPods(clusterSelector: ResourceSelector, options: PodReplicas): Result<Set<String>> =
        ClusterCreatePods(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun deletePods(clusterSelector: ResourceSelector, options: DeleteOptions) : Result<Void?> =
        ClusterDeletePods(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, options)

    fun stopJobs(clusterSelector: ResourceSelector, excludeJobIds: Set<String>): Result<Boolean> =
        ClusterStopJobs(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, excludeJobIds)

    fun stopJob(clusterSelector: ResourceSelector, context: JobContext): Result<Void?> =
        JobStop(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, null)

    fun cancelJob(clusterSelector: ResourceSelector, options: SavepointOptions, context: JobContext): Result<SavepointRequest?> =
        JobCancel(flinkOptions, flinkClient, kubeClient, context).execute(clusterSelector, options)

    fun isClusterReady(clusterSelector: ResourceSelector, slots: Int): Result<Boolean> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, slots)

    fun isClusterHealthy(clusterSelector: ResourceSelector): Result<Boolean> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, 0)

    fun getJobStatus(jobSelector: ResourceSelector, context: JobContext): Result<String?> =
        JobGetStatus(flinkOptions, flinkClient, kubeClient, context).execute(jobSelector, null)

    fun getTaskManagerStatus(clusterSelector: ResourceSelector) : Result<TaskManagersInfo?> =
        TaskManagersStatus(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, null)

    fun createPod(clusterSelector: ResourceSelector, pod: V1Pod): Result<String?> =
        PodCreate(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, pod)

    fun deletePod(clusterSelector: ResourceSelector, name: String): Result<Void?> =
        PodDelete(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, name)

    fun getClusterJobStatus(clusterSelector: ResourceSelector, jobId: String) =
        ClusterGetJobStatus(flinkOptions, flinkClient, kubeClient).execute(clusterSelector, jobId)

    fun updateStatus(clusterSelector: ResourceSelector, flinkCluster: V2FlinkCluster) {
        kubeClient.updateClusterStatus(clusterSelector, flinkCluster.status)
    }

    fun updateAnnotations(clusterSelector: ResourceSelector, flinkCluster: V2FlinkCluster) {
        kubeClient.updateClusterAnnotations(clusterSelector, flinkCluster.metadata?.annotations?.toMap() ?: mapOf())
    }

    fun updateFinalizers(clusterSelector: ResourceSelector, flinkCluster: V2FlinkCluster) {
        kubeClient.updateClusterFinalizers(clusterSelector, flinkCluster.metadata?.finalizers?.toList() ?: listOf())
    }

    fun updateStatus(jobSelector: ResourceSelector, flinkJob: V1FlinkJob) {
        kubeClient.updateJobStatus(jobSelector, flinkJob.status)
    }

    fun updateAnnotations(jobSelector: ResourceSelector, flinkJob: V1FlinkJob) {
        kubeClient.updateJobAnnotations(jobSelector, flinkJob.metadata?.annotations?.toMap() ?: mapOf())
    }

    fun updateFinalizers(jobSelector: ResourceSelector, flinkJob: V1FlinkJob) {
        kubeClient.updateJobFinalizers(jobSelector, flinkJob.metadata?.finalizers?.toList() ?: listOf())
    }

    fun updateAnnotations(clusterSelector: ResourceSelector, deployment: V1Deployment) {
        kubeClient.updateClusterAnnotations(clusterSelector, deployment.metadata?.annotations?.toMap() ?: mapOf())
    }

    fun updateTaskManagerReplicas(clusterSelector: ResourceSelector, requiredTaskManagers: Int) {
        kubeClient.rescaleCluster(clusterSelector, requiredTaskManagers)
    }

    fun updateJobParallelism(jobSelector: ResourceSelector, requiredJobParallelism: Int) {
        kubeClient.rescaleJob(jobSelector, requiredJobParallelism)
    }

    fun getFlinkJob(jobSelector: ResourceSelector): V1FlinkJob {
        return kubeClient.getFlinkJob(jobSelector)
    }

    fun isDryRun() = dryRun
}