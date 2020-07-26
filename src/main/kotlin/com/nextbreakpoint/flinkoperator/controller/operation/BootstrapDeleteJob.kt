package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class BootstrapDeleteJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapDeleteJob::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<Void?> {
        return try {
            kubeClient.deleteBootstrapJobs(clusterSelector)

            kubeClient.deleteBootstrapPods(clusterSelector)

            OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't delete bootstrap job", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}