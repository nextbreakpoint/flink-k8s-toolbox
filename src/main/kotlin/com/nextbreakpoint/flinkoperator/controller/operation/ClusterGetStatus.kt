package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class ClusterGetStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val cache: Cache) : Operation<Void?, Map<String, String>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterGetStatus::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Map<String, String>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterState = flinkCluster.status

            val result = mapOf(
                "timestamp" to (clusterState.timestamp?.toString() ?: ""),
                "clusterStatus" to (clusterState.clusterStatus ?: ""),
                "taskStatus" to (clusterState.taskStatus ?: ""),
                "tasks" to (clusterState.tasks?.joinToString(" ") ?: ""),
                "taskAttempts" to (clusterState.taskAttempts?.toString() ?: ""),
                "savepointPath" to (clusterState.savepointPath ?: ""),
                "savepointTimestamp" to (clusterState.savepointTimestamp?.toString() ?: ""),
                "savepointJobId" to (clusterState.savepointJobId ?: ""),
                "savepointTriggerId" to (clusterState.savepointTriggerId ?: ""),
                "runtimeDigest" to (clusterState.digest?.runtime ?: ""),
                "bootstrapDigest" to (clusterState.digest?.bootstrap ?: ""),
                "jobManagerDigest" to (clusterState.digest?.jobManager ?: ""),
                "taskManagerDigest" to (clusterState.digest?.taskManager ?: "")
            )

            return Result(
                ResultStatus.SUCCESS,
                result
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get annotations", e)

            return Result(
                ResultStatus.FAILED,
                mapOf()
            )
        }
    }
}