package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.models.V1PodList
import org.apache.log4j.Logger

class PodsAreTerminated(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(PodsAreTerminated::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val pods = listPods(Kubernetes.coreApi, "flink-operator", clusterId)

            if (pods.items.isEmpty()) {
                return Result(ResultStatus.SUCCESS, null)
            } else {
                return Result(ResultStatus.AWAIT, null)
            }
        } catch (e : Exception) {
            return Result(ResultStatus.FAILED, null)
        }
    }

    private fun listPods(api: CoreV1Api, owner: String, clusterId: ClusterId): V1PodList {
        return api.listNamespacedPod(
            clusterId.namespace,
            null,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=$owner",
            null,
            null,
            30,
            null
        )
    }
}