package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class PodsAreTerminated(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(PodsAreTerminated::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val jobmanagerPods = Kubernetes.coreApi.listNamespacedPod(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},uid=${clusterId.uuid},owner=${"flink-operator"},role=jobmanager",
                null,
                null,
                30,
                null
            )

            val taskmanagerPods = Kubernetes.coreApi.listNamespacedPod(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},uid=${clusterId.uuid},owner=${"flink-operator"},role=taskmanager",
                null,
                null,
                30,
                null
            )

            if (jobmanagerPods.items.isEmpty() && taskmanagerPods.items.isEmpty()) {
                return Result(ResultStatus.SUCCESS, null)
            } else {
                return Result(ResultStatus.AWAIT, null)
            }
        } catch (e : Exception) {
            logger.error("Can't get pods of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}