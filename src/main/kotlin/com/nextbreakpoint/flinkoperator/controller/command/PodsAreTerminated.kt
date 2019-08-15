package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import io.kubernetes.client.models.V1ContainerState
import io.kubernetes.client.models.V1ContainerStateRunning
import org.apache.log4j.Logger

class PodsAreTerminated(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(PodsAreTerminated::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val jobmanagerPods = KubernetesUtils.coreApi.listNamespacedPod(
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

            val taskmanagerPods = KubernetesUtils.coreApi.listNamespacedPod(
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
                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                if (jobmanagerPods.items.filter { it.status.containerStatuses.filter { it.lastState.running != null }.isNotEmpty() }.isNotEmpty()) {
                    return Result(
                        ResultStatus.AWAIT,
                        null
                    )
                }

                if (taskmanagerPods.items.filter { it.status.containerStatuses.filter { it.lastState.running != null }.isNotEmpty() }.isNotEmpty()) {
                    return Result(
                        ResultStatus.AWAIT,
                        null
                    )
                }

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("Can't get pods of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}