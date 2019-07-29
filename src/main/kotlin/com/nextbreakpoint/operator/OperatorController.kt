package com.nextbreakpoint.operator

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.*
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.command.*
import com.nextbreakpoint.operator.resources.ClusterResources

class OperatorController(val flinkOptions: FlinkOptions, val savepointInterval: Int) {
    fun startCluster(clusterId: ClusterId, options: StartOptions, cache: OperatorCache) : Result<List<OperatorTask>> =
        ClusterStart(flinkOptions, cache).execute(clusterId, options)

    fun stopCluster(clusterId: ClusterId, options: StopOptions, cache: OperatorCache) : Result<List<OperatorTask>> =
        ClusterStop(flinkOptions, cache).execute(clusterId, options)

    fun getClusterTask(clusterId: ClusterId, cache: OperatorCache) : Result<Map<String, String>> =
        ClusterGetTask(flinkOptions, cache).execute(clusterId, null)

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

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : Result<Map<String, String>> =
        SavepointTrigger(flinkOptions).execute(clusterId, options)

    fun getSavepointStatus(clusterId: ClusterId, savepointRequest: Map<String, String>) : Result<String> =
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

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): Result<Map<String, String>> =
        JobCancel(flinkOptions).execute(clusterId, options)

    fun stopJob(clusterId: ClusterId): Result<Map<String, String>> =
        JobStop(flinkOptions).execute(clusterId, null)

    fun isClusterReady(clusterId: ClusterId): Result<Void?> =
        ClusterIsReady(flinkOptions).execute(clusterId, null)

    fun isClusterRunning(clusterId: ClusterId): Result<Void?> =
        ClusterIsRunning(flinkOptions).execute(clusterId, null)

    fun isClusterSuspended(clusterId: ClusterId): Result<Void?> =
        ClusterIsSuspended(flinkOptions).execute(clusterId, null)

    fun isClusterTerminated(clusterId: ClusterId): Result<Void?> =
        ClusterIsTerminated(flinkOptions).execute(clusterId, null)

    fun isJobStarted(clusterId: ClusterId): Result<Void?> =
        JobHasStarted(flinkOptions).execute(clusterId, null)

    fun isJobStopped(clusterId: ClusterId): Result<Void?> =
        JobHasStopped(flinkOptions).execute(clusterId, null)

    fun uploadJar(clusterId: ClusterId, clusterResources: ClusterResources) : Result<Void?> =
        JarUpload(flinkOptions).execute(clusterId, clusterResources)

    fun updateAnnotations(flinkCluster: V1FlinkCluster) {
        Kubernetes.updateAnnotations(flinkCluster)
    }
}