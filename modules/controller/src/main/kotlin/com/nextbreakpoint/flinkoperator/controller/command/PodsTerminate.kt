package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import io.kubernetes.client.apis.AppsV1Api
import org.apache.log4j.Logger

class PodsTerminate(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(PodsTerminate::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            logger.info("Terminating pods of cluster ${clusterId.name}...")

            terminateStatefulSets(KubernetesUtils.appsApi, "flink-operator", clusterId)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't terminate pods of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun terminateStatefulSets(api: AppsV1Api, owner: String, clusterId: ClusterId) {
        val statefulSets = api.listNamespacedStatefulSet(
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

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Scaling StatefulSet ${statefulSet.metadata.name}...")

                val patch = listOf(
                    mapOf<String, Any?>(
                        "op" to "replace",
                        "path" to "/spec/replicas",
                        "value" to 0
                    )
                )

                val response = api.patchNamespacedStatefulSetScaleCall(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    patch,
                    null,
                    null,
                    null,
                    null
                ).execute()

                if (response.isSuccessful) {
                    logger.info("StatefulSet ${statefulSet.metadata.name} scaled")
                } else {
                    logger.warn("Can't scale StatefulSet ${statefulSet.metadata.name}")
                }
            } catch (e: Exception) {
                logger.warn("Failed to scale StatefulSet ${statefulSet.metadata.name}", e)
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }
}