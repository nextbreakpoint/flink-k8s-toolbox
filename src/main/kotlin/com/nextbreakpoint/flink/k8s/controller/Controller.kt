package com.nextbreakpoint.flink.k8s.controller

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobManagerStats
import com.nextbreakpoint.flink.common.JobStats
import com.nextbreakpoint.flink.common.RunJarOptions
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.common.TaskManagerStats
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.action.BatchJobCreate
import com.nextbreakpoint.flink.k8s.controller.action.BatchJobDelete
import com.nextbreakpoint.flink.k8s.controller.action.ClusterIsReady
import com.nextbreakpoint.flink.k8s.controller.action.ClusterListJars
import com.nextbreakpoint.flink.k8s.controller.action.ClusterRemoveJars
import com.nextbreakpoint.flink.k8s.controller.action.ClusterRunJar
import com.nextbreakpoint.flink.k8s.controller.action.ClusterStopJobs
import com.nextbreakpoint.flink.k8s.controller.action.ClusterUploadJar
import com.nextbreakpoint.flink.k8s.controller.action.DeploymentCreate
import com.nextbreakpoint.flink.k8s.controller.action.DeploymentDelete
import com.nextbreakpoint.flink.k8s.controller.action.FlinkClusterCreate
import com.nextbreakpoint.flink.k8s.controller.action.FlinkClusterDelete
import com.nextbreakpoint.flink.k8s.controller.action.FlinkClusterGetStatus
import com.nextbreakpoint.flink.k8s.controller.action.FlinkClusterUpdate
import com.nextbreakpoint.flink.k8s.controller.action.FlinkJobCreate
import com.nextbreakpoint.flink.k8s.controller.action.FlinkJobDelete
import com.nextbreakpoint.flink.k8s.controller.action.FlinkJobGetStatus
import com.nextbreakpoint.flink.k8s.controller.action.FlinkJobUpdate
import com.nextbreakpoint.flink.k8s.controller.action.JobCancel
import com.nextbreakpoint.flink.k8s.controller.action.JobDetails
import com.nextbreakpoint.flink.k8s.controller.action.JobGetStatus
import com.nextbreakpoint.flink.k8s.controller.action.JobManagerMetrics
import com.nextbreakpoint.flink.k8s.controller.action.JobMetrics
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
import com.nextbreakpoint.flink.k8s.controller.action.ServiceCreate
import com.nextbreakpoint.flink.k8s.controller.action.ServiceDelete
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagerDetails
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagerMetrics
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagersList
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagersStatus
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import com.nextbreakpoint.flink.k8s.factory.DeploymentResourcesDefaultFactory
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo
import com.nextbreakpoint.flinkclient.model.TaskManagerDetailsInfo
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
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

    fun requestScaleCluster(namespace: String, clusterName: String, options: ScaleClusterOptions): Result<Void?> =
        RequestClusterScale(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, options)

    fun requestStartCluster(namespace: String, clusterName: String, options: StartOptions, context: ClusterContext) : Result<Void?> =
        RequestClusterStart(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, options)

    fun requestStopCluster(namespace: String, clusterName: String, options: StopOptions, context: ClusterContext) : Result<Void?> =
        RequestClusterStop(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, options)

    fun requestScaleJob(namespace: String, clusterName: String, jobName: String, options: ScaleJobOptions): Result<Void?> =
        RequestJobScale(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, options)

    fun requestStartJob(namespace: String, clusterName: String, jobName: String, options: StartOptions, context: JobContext) : Result<Void?> =
        RequestJobStart(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName, options)

    fun requestStopJob(namespace: String, clusterName: String, jobName: String, options: StopOptions, context: JobContext) : Result<Void?> =
        RequestJobStop(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName, options)

    fun requestTriggerSavepoint(namespace: String, clusterName: String, jobName: String, context: JobContext) : Result<Void?> =
        RequestSavepointTrigger(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName,null)

    fun requestForgetSavepoint(namespace: String, clusterName: String, jobName: String, context: JobContext) : Result<Void?> =
        RequestSavepointForget(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName,null)

    fun getFlinkClusterStatus(namespace: String, clusterName: String, context: ClusterContext) : Result<V1FlinkClusterStatus?> =
        FlinkClusterGetStatus(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, null)

    fun createFlinkCluster(namespace: String, clusterName: String, flinkCluster: V1FlinkCluster) : Result<Void?> =
        FlinkClusterCreate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, flinkCluster)

    fun deleteFlinkCluster(namespace: String, clusterName: String) : Result<Void?> =
        FlinkClusterDelete(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,null)

    fun updateFlinkCluster(namespace: String, clusterName: String, flinkCluster: V1FlinkCluster) : Result<Void?> =
        FlinkClusterUpdate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, flinkCluster)

    fun createService(namespace: String, clusterName: String, service: V1Service): Result<String?> =
        ServiceCreate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, service)

    fun deleteService(namespace: String, clusterName: String, name: String): Result<Void?> =
        ServiceDelete(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, name)

    fun createDeployment(namespace: String, clusterName: String, deployment: V1Deployment) : Result<String?> =
        DeploymentCreate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, deployment)

    fun deleteDeployment(namespace: String, clusterName: String, name: String) : Result<Void?> =
        DeploymentDelete(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, name)

    fun removeJars(namespace: String, clusterName: String) : Result<Void?> =
        ClusterRemoveJars(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,null)

    fun listJars(namespace: String, clusterName: String) : Result<List<JarFileInfo>> =
        ClusterListJars(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,null)

    fun runJar(namespace: String, clusterName: String, options: RunJarOptions) : Result<String> =
        ClusterRunJar(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, options)

    fun uploadJar(namespace: String, clusterName: String, file: File): Result<JarUploadResponseBody?> =
        ClusterUploadJar(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, file)

    fun triggerSavepoint(namespace: String, clusterName: String, jobName: String, options: SavepointOptions, context: JobContext) : Result<SavepointRequest?> =
        SavepointTrigger(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName, options)

    fun querySavepoint(namespace: String, clusterName: String, jobName: String, savepointRequest: SavepointRequest, context: JobContext) : Result<String?> =
        SavepointQuery(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName, savepointRequest)

    fun createBootstrapJob(namespace: String, clusterName: String, jobName: String, bootstrapJob: V1Job): Result<String?> =
        BatchJobCreate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, bootstrapJob)

    fun deleteBootstrapJob(namespace: String, clusterName: String, jobName: String, name: String) : Result<Void?> =
        BatchJobDelete(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, name)

    fun createFlinkJob(namespace: String, clusterName: String, jobName: String, job: V1FlinkJob): Result<Void?> =
        FlinkJobCreate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, job)

    fun deleteFlinkJob(namespace: String, clusterName: String, jobName: String) : Result<Void?> =
        FlinkJobDelete(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, null)

    fun updateFlinkJob(namespace: String, clusterName: String, jobName: String, job: V1FlinkJob) : Result<Void?> =
        FlinkJobUpdate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, job)

    fun stopJobs(namespace: String, clusterName: String, excludeJobIds: Set<String>): Result<Boolean> =
        ClusterStopJobs(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, excludeJobIds)

    fun stopJob(namespace: String, clusterName: String, jobName: String, context: JobContext): Result<Void?> =
        JobStop(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName, null)

    fun cancelJob(namespace: String, clusterName: String, jobName: String, options: SavepointOptions, context: JobContext): Result<SavepointRequest?> =
        JobCancel(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName, options)

    fun getJobDetails(namespace: String, clusterName: String, jobName: String, context: JobContext): Result<JobDetailsInfo?> =
        JobDetails(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, context.getJobId())

    fun getJobMetrics(namespace: String, clusterName: String, jobName: String, context: JobContext): Result<JobStats?> =
        JobMetrics(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, context.getJobId())

    fun getJobManagerMetrics(namespace: String, clusterName: String) : Result<JobManagerStats?> =
        JobManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,null)

    fun getTaskManagerStatus(namespace: String, clusterName: String) : Result<TaskManagersInfo?> =
        TaskManagersStatus(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,null)

    fun getTaskManagersList(namespace: String, clusterName: String) : Result<List<TaskManagerInfo>> =
        TaskManagersList(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,null)

    fun getTaskManagerDetails(namespace: String, clusterName: String, taskmanagerId: TaskManagerId) : Result<TaskManagerDetailsInfo?> =
        TaskManagerDetails(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, taskmanagerId)

    fun getTaskManagerMetrics(namespace: String, clusterName: String, taskmanagerId: TaskManagerId) : Result<TaskManagerStats?> =
        TaskManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, taskmanagerId)

    fun createPod(namespace: String, clusterName: String, pod: V1Pod): Result<String?> =
        PodCreate(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, pod)

    fun deletePod(namespace: String, clusterName: String, name: String): Result<Void?> =
        PodDelete(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, name)

    fun isClusterReady(namespace: String, clusterName: String, slots: Int): Result<Boolean> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, slots)

    fun isClusterHealthy(namespace: String, clusterName: String): Result<Boolean> =
        ClusterIsReady(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName,0)

    fun getFlinkJobStatus(namespace: String, clusterName: String, jobName: String, context: JobContext): Result<V1FlinkJobStatus?> =
        FlinkJobGetStatus(flinkOptions, flinkClient, kubeClient, context).execute(namespace, clusterName, jobName,null)

    fun getJobStatus(namespace: String, clusterName: String, jobName: String, jobId: String) =
        JobGetStatus(flinkOptions, flinkClient, kubeClient).execute(namespace, clusterName, jobName, jobId)


    fun updateStatus(namespace: String, name: String, flinkCluster: V1FlinkCluster) {
        kubeClient.updateClusterStatus(namespace, name, flinkCluster.status)
    }

    fun updateAnnotations(namespace: String, name: String, flinkCluster: V1FlinkCluster) {
        kubeClient.updateClusterAnnotations(namespace, name, flinkCluster.metadata?.annotations?.toMap() ?: mapOf())
    }

    fun updateFinalizers(namespace: String, name: String, flinkCluster: V1FlinkCluster) {
        kubeClient.updateClusterFinalizers(namespace, name, flinkCluster.metadata?.finalizers?.toList() ?: listOf())
    }

    fun updateStatus(namespace: String, name: String, flinkJob: V1FlinkJob) {
        kubeClient.updateJobStatus(namespace, name, flinkJob.status)
    }

    fun updateAnnotations(namespace: String, name: String, flinkJob: V1FlinkJob) {
        kubeClient.updateJobAnnotations(namespace, name, flinkJob.metadata?.annotations?.toMap() ?: mapOf())
    }

    fun updateFinalizers(namespace: String, name: String, flinkJob: V1FlinkJob) {
        kubeClient.updateJobFinalizers(namespace, name, flinkJob.metadata?.finalizers?.toList() ?: listOf())
    }

    fun updateStatus(namespace: String, name: String, flinkDeployment: V1FlinkDeployment) {
        kubeClient.updateDeploymentStatus(namespace, name, flinkDeployment.status)
    }

    fun updateAnnotations(namespace: String, name: String, flinkDeployment: V1FlinkDeployment) {
        kubeClient.updateDeploymentAnnotations(namespace, name, flinkDeployment.metadata?.annotations?.toMap() ?: mapOf())
    }

    fun updateFinalizers(namespace: String, name: String, flinkDeployment: V1FlinkDeployment) {
        kubeClient.updateDeploymentFinalizers(namespace, name, flinkDeployment.metadata?.finalizers?.toList() ?: listOf())
    }

    fun updateTaskManagerReplicas(namespace: String, name: String, requiredTaskManagers: Int) {
        kubeClient.rescaleCluster(namespace, name, requiredTaskManagers)
    }

    fun updateJobParallelism(namespace: String, name: String, requiredJobParallelism: Int) {
        kubeClient.rescaleJob(namespace, name, requiredJobParallelism)
    }

    fun getFlinkDeployment(namespace: String, name: String): V1FlinkDeployment {
        return kubeClient.getFlinkDeployment(namespace, name)
    }

    fun getFlinkCluster(namespace: String, name: String): V1FlinkCluster {
        return kubeClient.getFlinkCluster(namespace, name)
    }

    fun getFlinkJob(namespace: String, name: String): V1FlinkJob {
        return kubeClient.getFlinkJob(namespace, name)
    }

    fun makeFlinkCluster(namespace: String, clusterName: String, clusterSpec: V1FlinkClusterSpec): V1FlinkCluster {
        return DeploymentResourcesDefaultFactory.createFlinkCluster(namespace, Resource.RESOURCE_OWNER, clusterName, clusterSpec)
    }

    fun makeFlinkJob(namespace: String, clusterName: String, jobName: String, jobSpec: V1FlinkJobSpec): V1FlinkJob {
        return DeploymentResourcesDefaultFactory.createFlinkJob(namespace, Resource.RESOURCE_OWNER, clusterName, jobName, jobSpec)
    }

    fun isDryRun() = dryRun
}