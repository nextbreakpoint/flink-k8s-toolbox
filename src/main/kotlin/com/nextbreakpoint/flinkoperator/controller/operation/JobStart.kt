package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class JobStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStart::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1FlinkCluster): OperationResult<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getOverview(address)

            if (overview.jobsRunning > 1) {
                logger.warn("[name=${clusterSelector.name}] There are multiple jobs running")
            }

            if (overview.jobsRunning > 0) {
                logger.warn("[name=${clusterSelector.name}] Job already running!")

                return OperationResult(
                    OperationStatus.OK,
                    null
                )
            }

            val files = flinkClient.listJars(address)

            val jarFile = files.maxBy { it.uploaded }

            if (jarFile == null) {
                logger.warn("[name=${clusterSelector.name}] Can't find any JAR file")

                return OperationResult(
                    OperationStatus.ERROR,
                    null
                )
            }

            val savepointPath = params.status.savepointPath
            val parallelism = params.status.jobParallelism
            val className = params.spec.bootstrap.className
            val arguments = params.spec.bootstrap.arguments

            flinkClient.runJar(address, jarFile, className, parallelism, savepointPath, arguments)

            logger.debug("[name=${clusterSelector.name}] Job started")

            return OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.warn("[name=${clusterSelector.name}] Can't start job")

            return OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}