package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class JobStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkClient.getOverview(address)

            if (overview.jobsRunning > 0) {
                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            val files = flinkClient.listJars(address)

            val jarFile = files.maxBy { it.uploaded }

            if (jarFile == null) {
                logger.warn("[name=${clusterId.name}] Can't find any JAR file")

                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            val savepointPath = params.status.savepointPath
            val parallelism = params.status.jobParallelism

            flinkClient.runJar(address, jarFile, params.spec.bootstrap, parallelism, savepointPath)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.warn("[name=${clusterId.name}] Can't get JAR files")

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}