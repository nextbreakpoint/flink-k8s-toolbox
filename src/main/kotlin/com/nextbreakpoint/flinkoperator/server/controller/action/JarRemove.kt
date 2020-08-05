package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class JarRemove(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JarRemove::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<Void?> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val files = flinkClient.listJars(address)

            if (files.isNotEmpty()) {
                flinkClient.deleteJars(address, files)
            }

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't remove JAR files", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}