package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class TaskManagersGetReplicas(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Int>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersGetReplicas::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Int> {
        try {
            val replicas = kubeClient.getTaskManagerStatefulSetReplicas(clusterId)

            return Result(
                ResultStatus.SUCCESS,
                replicas
            )
        } catch (e : Exception) {
            logger.error("Can't get replicas of task managers of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                0
            )
        }
    }
}