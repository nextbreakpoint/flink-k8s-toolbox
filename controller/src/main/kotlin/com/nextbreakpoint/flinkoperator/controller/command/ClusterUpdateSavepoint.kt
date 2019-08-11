package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import io.kubernetes.client.apis.CustomObjectsApi
import org.apache.log4j.Logger

class ClusterUpdateSavepoint(flinkOptions: FlinkOptions) : OperatorCommand<String, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterUpdateSavepoint::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: String): Result<Void?> {
        try {
            logger.info("Updating savepoint of cluster ${clusterId.name}...")

            updateSavepoint(KubernetesUtils.objectApi, clusterId, params)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't update savepoint of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun updateSavepoint(api: CustomObjectsApi, clusterId: ClusterId, savepointPath: String) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "flinkOperator" to mapOf<String, Any?>(
                    "savepointPath" to savepointPath
                )
            )
        )

        val response = api.patchNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            clusterId.namespace,
            "flinkclusters",
            clusterId.name,
            patch,
            null,
            null
        ).execute()

        if (response.isSuccessful) {
            logger.info("Savepoint of cluster ${clusterId.name} updated to $savepointPath")
        } else {
            logger.error("Can't update savepoint of cluster ${clusterId.name}")
        }
    }
}