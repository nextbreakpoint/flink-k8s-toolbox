package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import org.apache.log4j.Logger

class ClusterScale(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: OperatorCache) : OperatorCommand<ScaleOptions, List<OperatorTask>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterScale::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ScaleOptions): Result<List<OperatorTask>> {
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

            val statusList = tryScalingCluster(flinkCluster, params)

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

            OperatorState.setTaskManagers(flinkCluster, params.taskManagers)
            OperatorState.setTaskSlots(flinkCluster, params.taskSlots)
            OperatorState.setJobParallelism(flinkCluster, params.taskManagers * params.taskSlots)

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

    private fun tryScalingCluster(flinkCluster: V1FlinkCluster, params: ScaleOptions): List<OperatorTask> {
        val clusterStatus = OperatorState.getClusterStatus(flinkCluster)

        val bootstrapSpec = flinkCluster.spec?.bootstrap

        return if (bootstrapSpec == null) {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.taskManagers > 0) {
                        listOf(
                            OperatorTask.RescaleCluster,
                            OperatorTask.ClusterRunning
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
                    if (params.taskManagers > 0) {
                        listOf(
                            OperatorTask.StoppingCluster,
                            OperatorTask.CancelJob,
                            OperatorTask.RescaleCluster,
                            OperatorTask.StartingCluster,
                            OperatorTask.DeleteBootstrapJob,
                            OperatorTask.CreateBootstrapJob,
                            OperatorTask.StartJob,
                            OperatorTask.ClusterRunning
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
                else -> listOf()
            }
        }
    }
}