package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class JobStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): OperationResult<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkClient.getOverview(address)

            if (overview.jobsRunning > 1) {
                logger.warn("[name=${clusterId.name}] There are multiple jobs running")
            }

            if (overview.jobsRunning > 0) {
                logger.warn("[name=${clusterId.name}] Job already running!")

                return OperationResult(
                    OperationStatus.COMPLETED,
                    null
                )
            }

            val files = flinkClient.listJars(address)

            val jarFile = files.maxBy { it.uploaded }

            if (jarFile == null) {
                logger.warn("[name=${clusterId.name}] Can't find any JAR file")

                return OperationResult(
                    OperationStatus.RETRY,
                    null
                )
            }

            val savepointPath = params.status.savepointPath
            val parallelism = params.status.jobParallelism
            val className = params.spec.bootstrap.className
            val arguments = params.spec.bootstrap.arguments

            flinkClient.runJar(address, jarFile, className, parallelism, savepointPath, arguments)

            logger.debug("[name=${clusterId.name}] Job started")

            return OperationResult(
                OperationStatus.COMPLETED,
                null
            )
        } catch (e : Exception) {
            logger.warn("[name=${clusterId.name}] Can't start job")

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}