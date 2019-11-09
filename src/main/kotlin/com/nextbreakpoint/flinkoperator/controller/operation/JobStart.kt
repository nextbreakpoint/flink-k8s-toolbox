package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import org.apache.log4j.Logger

class JobStart(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<V1FlinkCluster, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkContext.getOverview(address)

            if (overview.jobsRunning > 0) {
                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            val files = flinkContext.listJars(address)

            val jarFile = files.maxBy { it.uploaded }

            if (jarFile == null) {
                logger.warn("Can't find any JAR file in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            val savepointPath = OperatorParameters.getSavepointPath(params)

            val taskManagers = params.spec?.taskManagers ?: 1
            val taskSlots = params.spec.taskManager?.taskSlots ?: 1
            val parallelism = taskManagers * taskSlots

            flinkContext.runJar(address, jarFile, params.spec.flinkJob, parallelism, savepointPath)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.warn("Can't get JAR files of cluster ${clusterId.name}")

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}