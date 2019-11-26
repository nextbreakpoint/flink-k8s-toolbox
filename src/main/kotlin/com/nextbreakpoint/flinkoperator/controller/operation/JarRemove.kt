package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class JarRemove(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JarRemove::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val files = flinkClient.listJars(address)

            if (files.isNotEmpty()) {
                flinkClient.deleteJars(address, files)
            }

            return OperationResult(
                OperationStatus.COMPLETED,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't remove JAR files", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}