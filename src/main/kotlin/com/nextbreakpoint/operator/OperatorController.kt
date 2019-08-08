package com.nextbreakpoint.operator

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.SavepointOptions
import com.nextbreakpoint.common.model.SavepointRequest
import com.nextbreakpoint.common.model.StartOptions
import com.nextbreakpoint.common.model.StopOptions
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.command.ClusterCreateResources
import com.nextbreakpoint.operator.command.ClusterDeleteResources
import com.nextbreakpoint.operator.command.ClusterGetStatus
import com.nextbreakpoint.operator.command.ClusterIsReady
import com.nextbreakpoint.operator.command.ClusterIsRunning
import com.nextbreakpoint.operator.command.ClusterIsSuspended
import com.nextbreakpoint.operator.command.ClusterIsTerminated
import com.nextbreakpoint.operator.command.ClusterStart
import com.nextbreakpoint.operator.command.ClusterStop
import com.nextbreakpoint.operator.command.SavepointCreate
import com.nextbreakpoint.operator.command.ClusterUpdateSavepoint
import com.nextbreakpoint.operator.command.ClusterUpdateStatus
import com.nextbreakpoint.operator.command.FlinkClusterCreate
import com.nextbreakpoint.operator.command.FlinkClusterDelete
import com.nextbreakpoint.operator.command.JarIsReady
import com.nextbreakpoint.operator.command.JarRemove
import com.nextbreakpoint.operator.command.JarRun
import com.nextbreakpoint.operator.command.JarUpload
import com.nextbreakpoint.operator.command.JobCancel
import com.nextbreakpoint.operator.command.JobHasStarted
import com.nextbreakpoint.operator.command.JobHasStopped
import com.nextbreakpoint.operator.command.JobStop
import com.nextbreakpoint.operator.command.PodsAreTerminated
import com.nextbreakpoint.operator.command.PodsRestart
import com.nextbreakpoint.operator.command.PodsTerminate
import com.nextbreakpoint.operator.command.SavepointGetStatus
import com.nextbreakpoint.operator.command.SavepointTrigger
import com.nextbreakpoint.operator.command.UploadJobDeleteResource
import com.nextbreakpoint.operator.resources.ClusterResources

class OperatorController(val flinkOptions: FlinkOptions) {
    fun startCluster(clusterId: ClusterId, options: StartOptions, cache: OperatorCache) : Result<List<OperatorTask>> =
        ClusterStart(flinkOptions, cache).execute(clusterId, options)

    fun stopCluster(clusterId: ClusterId, options: StopOptions, cache: OperatorCache) : Result<List<OperatorTask>> =
        ClusterStop(flinkOptions, cache).execute(clusterId, options)

    fun createSavepoint(clusterId: ClusterId, cache: OperatorCache) : Result<List<OperatorTask>> =
        SavepointCreate(flinkOptions, cache).execute(clusterId, null)

    fun getClusterStatus(clusterId: ClusterId, cache: OperatorCache) : Result<Map<String, String>> =
        ClusterGetStatus(flinkOptions, cache).execute(clusterId, null)

    fun createFlinkCluster(clusterId: ClusterId, flinkCluster: V1FlinkCluster) : Result<Void?> =
        FlinkClusterCreate(flinkOptions).execute(clusterId, flinkCluster)

    fun deleteFlinkCluster(clusterId: ClusterId) : Result<Void?> =
        FlinkClusterDelete(flinkOptions).execute(clusterId, null)

    fun updateClusterStatus(clusterId: ClusterId, flinkCluster: V1FlinkCluster, resources: OperatorResources) : Result<Void?> =
        ClusterUpdateStatus(this, resources).execute(clusterId, flinkCluster)

    fun createClusterResources(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        ClusterCreateResources(flinkOptions).execute(clusterId, clusterResources)

    fun removeJar(clusterId: ClusterId) : Result<Void?> =
        JarRemove(flinkOptions).execute(clusterId, null)

    fun isJarReady(clusterId: ClusterId) : Result<Void?> =
        JarIsReady(flinkOptions).execute(clusterId, null)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : Result<SavepointRequest?> =
        SavepointTrigger(flinkOptions).execute(clusterId, options)

    fun getSavepointStatus(clusterId: ClusterId, savepointRequest: SavepointRequest) : Result<String> =
        SavepointGetStatus(flinkOptions).execute(clusterId, savepointRequest)

    fun deleteClusterResources(clusterId: ClusterId) : Result<Void?> =
        ClusterDeleteResources(flinkOptions).execute(clusterId, null)

    fun deleteUploadJobResource(clusterId: ClusterId) : Result<Void?> =
        UploadJobDeleteResource(flinkOptions).execute(clusterId, null)

    fun terminatePods(clusterId: ClusterId) : Result<Void?> =
        PodsTerminate(flinkOptions).execute(clusterId, null)

    fun restartPods(clusterId: ClusterId, clusterResources: ClusterResources): Result<Void?> =
        PodsRestart(flinkOptions).execute(clusterId, clusterResources)

    fun arePodsTerminated(clusterId: ClusterId): Result<Void?> =
        PodsAreTerminated(flinkOptions).execute(clusterId, null)

    fun runJar(clusterId: ClusterId, cluster: V1FlinkCluster) : Result<Void?> =
        JarRun(flinkOptions).execute(clusterId, cluster)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): Result<SavepointRequest?> =
        JobCancel(flinkOptions).execute(clusterId, options)

    fun stopJob(clusterId: ClusterId): Result<Map<String, String>> =
        JobStop(flinkOptions).execute(clusterId, null)

    fun isClusterReady(clusterId: ClusterId): Result<Void?> =
        ClusterIsReady(flinkOptions).execute(clusterId, null)

    fun isClusterRunning(clusterId: ClusterId): Result<Boolean> =
        ClusterIsRunning(flinkOptions).execute(clusterId, null)

    fun isClusterSuspended(clusterId: ClusterId): Result<Void?> =
        ClusterIsSuspended(flinkOptions).execute(clusterId, null)

    fun isClusterTerminated(clusterId: ClusterId): Result<Void?> =
        ClusterIsTerminated(flinkOptions).execute(clusterId, null)

    fun isJobStarted(clusterId: ClusterId): Result<Void?> =
        JobHasStarted(flinkOptions).execute(clusterId, null)

    fun isJobStopped(clusterId: ClusterId): Result<Void?> =
        JobHasStopped(flinkOptions).execute(clusterId, null)

    fun uploadJar(clusterId: ClusterId, clusterResources: ClusterResources): Result<Void?> =
        JarUpload(flinkOptions).execute(clusterId, clusterResources)

    fun updateSavepoint(clusterId: ClusterId, savepointPath: String): Result<Void?> =
        ClusterUpdateSavepoint(flinkOptions).execute(clusterId, savepointPath)

    fun updateAnnotations(clusterId: ClusterId, annotations: Map<String, String>) {
        Kubernetes.updateAnnotations(clusterId, annotations)
    }
}