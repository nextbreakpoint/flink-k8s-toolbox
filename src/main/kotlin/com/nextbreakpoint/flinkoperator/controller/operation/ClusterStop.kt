package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import org.apache.log4j.Logger

class ClusterStop(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: OperatorCache) : OperatorCommand<StopOptions, List<OperatorTask>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StopOptions): Result<List<OperatorTask>> {
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

            val statusList = tryStoppingCluster(flinkCluster, params)

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

    private fun tryStoppingCluster(flinkCluster: V1FlinkCluster, params: StopOptions): List<OperatorTask> {
        val clusterStatus = OperatorState.getClusterStatus(flinkCluster)

        val jobSpec = flinkCluster.spec?.flinkJob

        return if (jobSpec == null) {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.deleteResources) {
                        listOf(
                            OperatorTask.StoppingCluster,
                            OperatorTask.TerminatePods,
                            OperatorTask.DeleteResources,
                            OperatorTask.TerminatedCluster,
                            OperatorTask.ClusterHalted
                        )
                    } else {
                        listOf(
                            OperatorTask.StoppingCluster,
                            OperatorTask.TerminatePods,
                            OperatorTask.SuspendCluster,
                            OperatorTask.ClusterHalted
                        )
                    }
                else -> listOf()
            }
        } else {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.deleteResources) {
                        if (params.withoutSavepoint) {
                            listOf(
                                OperatorTask.StoppingCluster,
                                OperatorTask.StopJob,
                                OperatorTask.TerminatePods,
                                OperatorTask.DeleteResources,
                                OperatorTask.TerminatedCluster,
                                OperatorTask.ClusterHalted
                            )
                        } else {
                            listOf(
                                OperatorTask.StoppingCluster,
                                OperatorTask.CancelJob,
                                OperatorTask.TerminatePods,
                                OperatorTask.DeleteResources,
                                OperatorTask.TerminatedCluster,
                                OperatorTask.ClusterHalted
                            )
                        }
                    } else {
                        if (params.withoutSavepoint) {
                            listOf(
                                OperatorTask.StoppingCluster,
                                OperatorTask.StopJob,
                                OperatorTask.TerminatePods,
                                OperatorTask.SuspendCluster,
                                OperatorTask.ClusterHalted
                            )
                        } else {
                            listOf(
                                OperatorTask.StoppingCluster,
                                OperatorTask.CancelJob,
                                OperatorTask.TerminatePods,
                                OperatorTask.SuspendCluster,
                                OperatorTask.ClusterHalted
                            )
                        }
                    }
                ClusterStatus.Suspended ->
                    listOf(
                        OperatorTask.StoppingCluster,
                        OperatorTask.TerminatePods,
                        OperatorTask.DeleteResources,
                        OperatorTask.TerminatedCluster,
                        OperatorTask.ClusterHalted
                    )
                ClusterStatus.Failed ->
                    listOf(
                        OperatorTask.StoppingCluster,
                        OperatorTask.TerminatePods,
                        OperatorTask.DeleteResources,
                        OperatorTask.TerminatedCluster,
                        OperatorTask.ClusterHalted
                    )
                else -> listOf()
            }
        }
    }
}