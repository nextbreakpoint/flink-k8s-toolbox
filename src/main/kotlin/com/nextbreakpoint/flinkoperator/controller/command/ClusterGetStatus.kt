package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class ClusterGetStatus(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: OperatorCache) : OperatorCommand<Void?, Map<String, String>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterGetStatus::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Map<String, String>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterState = flinkCluster.state

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
                "digestOfFlinkJob" to (clusterState.digestOfFlinkJob ?: ""),
                "digestOfFlinkImage" to (clusterState.digestOfFlinkImage ?: ""),
                "digestOfJobManager" to (clusterState.digestOfJobManager ?: ""),
                "digestOfTaskManager" to (clusterState.digestOfTaskManager ?: "")
            )

            return Result(
                ResultStatus.SUCCESS,
                result
            )
        } catch (e : Exception) {
            logger.error("Can't get annotations of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                mapOf()
            )
        }
    }
}