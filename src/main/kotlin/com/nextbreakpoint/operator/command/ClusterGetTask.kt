package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCache
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class ClusterGetTask(flinkOptions: FlinkOptions, val cache: OperatorCache) : OperatorCommand<Void?, Map<String, String>>(flinkOptions) {
    private val logger = Logger.getLogger(ClusterGetTask::class.simpleName)

    override fun execute(clusterId: ClusterId, params: Void?): Result<Map<String, String>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val result = flinkCluster.metadata.annotations?.filter { it.key.startsWith("flink-operator-") }?.toMap() ?: mapOf()

            return Result(ResultStatus.SUCCESS, result)
        } catch (e : Exception) {
            logger.error("Can't get status of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, mapOf())
        }
    }
}