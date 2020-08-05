package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class JobStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStart::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1FlinkCluster): Result<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getOverview(address)

            if (overview.jobsRunning > 1) {
                logger.warn("[name=${clusterSelector.name}] There are multiple jobs running")
            }

            if (overview.jobsRunning > 0) {
                logger.warn("[name=${clusterSelector.name}] Job already running!")

                return Result(
                    ResultStatus.OK,
                    null
                )
            }

            val files = flinkClient.listJars(address)

            val jarFile = files.maxBy { it.uploaded }

            if (jarFile == null) {
                logger.warn("[name=${clusterSelector.name}] Can't find any JAR file")

                return Result(
                    ResultStatus.ERROR,
                    null
                )
            }

            val savepointPath = params.status.savepointPath
            val parallelism = params.status.jobParallelism
            val className = params.spec.bootstrap.className
            val arguments = params.spec.bootstrap.arguments

            flinkClient.runJar(address, jarFile, className, parallelism, savepointPath, arguments)

            logger.debug("[name=${clusterSelector.name}] Job started")

            return Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.warn("[name=${clusterSelector.name}] Can't start job")

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}