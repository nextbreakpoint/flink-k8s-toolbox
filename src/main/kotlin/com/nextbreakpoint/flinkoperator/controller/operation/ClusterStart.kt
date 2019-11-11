package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import org.apache.log4j.Logger

class ClusterStart(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: OperatorCache) : OperatorCommand<StartOptions, List<OperatorTask>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StartOptions): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val operatorStatus = OperatorState.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus != TaskStatus.Idle) {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        OperatorState.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            val statusList = tryStartingCluster(flinkCluster, params)

            if (statusList.isEmpty()) {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        OperatorState.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            OperatorState.appendTasks(flinkCluster, statusList)

            return Result(
                ResultStatus.SUCCESS,
                statusList
            )
        } catch (e : Exception) {
            logger.error("Can't change tasks sequence of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                listOf()
            )
        }
    }

    private fun tryStartingCluster(flinkCluster: V1FlinkCluster, params: StartOptions): List<OperatorTask> {
        val clusterStatus = OperatorState.getClusterStatus(flinkCluster)

        val bootstrapSpec = flinkCluster.spec?.bootstrap

        return if (bootstrapSpec == null) {
            when (clusterStatus) {
                ClusterStatus.Terminated ->
                    listOf(
                        OperatorTask.StartingCluster,
                        OperatorTask.CreateResources,
                        OperatorTask.ClusterRunning
                    )
                ClusterStatus.Suspended ->
                    listOf(
                        OperatorTask.StartingCluster,
                        OperatorTask.RestartPods,
                        OperatorTask.ClusterRunning
                    )
                ClusterStatus.Failed ->
                    listOf(
                        OperatorTask.StoppingCluster,
                        OperatorTask.TerminatePods,
                        OperatorTask.DeleteResources,
                        OperatorTask.StartingCluster,
                        OperatorTask.CreateResources,
                        OperatorTask.ClusterRunning
                    )
                else -> listOf()
            }
        } else {
            when (clusterStatus) {
                ClusterStatus.Terminated ->
                    if (params.withoutSavepoint) {
                        listOf(
                            OperatorTask.StartingCluster,
                            OperatorTask.CreateResources,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.EraseSavepoint,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            OperatorTask.StartingCluster,
                            OperatorTask.CreateResources,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
                        )
                    }
                ClusterStatus.Suspended ->
                    if (params.withoutSavepoint) {
                        listOf(
                            OperatorTask.StartingCluster,
                            OperatorTask.RestartPods,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.EraseSavepoint,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            OperatorTask.StartingCluster,
                            OperatorTask.RestartPods,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
                        )
                    }
                ClusterStatus.Failed ->
                    if (params.withoutSavepoint) {
                        listOf(
                            OperatorTask.StoppingCluster,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.TerminatePods,
                            OperatorTask.DeleteResources,
                            OperatorTask.StartingCluster,
                            OperatorTask.CreateResources,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.EraseSavepoint,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            OperatorTask.StoppingCluster,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.TerminatePods,
                            OperatorTask.DeleteResources,
                            OperatorTask.StartingCluster,
                            OperatorTask.CreateResources,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
                        )
                    }
                else -> listOf()
            }
        }
    }
}