package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.Status
import org.apache.log4j.Logger

class ClusterScale(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val cache: Cache) : Operation<ClusterScaling, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterScaling::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterScaling): Result<List<ClusterTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val operatorStatus = Status.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus != TaskStatus.Idle) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return Result(
                    ResultStatus.AWAIT,
                    listOf()
                )
            }

            val statusList = tryScalingCluster(flinkCluster, params)

            if (statusList.isEmpty()) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return Result(
                    ResultStatus.AWAIT,
                    listOf()
                )
            }

            Status.setTaskManagers(flinkCluster, params.taskManagers)
            Status.setTaskSlots(flinkCluster, params.taskSlots)
            Status.setJobParallelism(flinkCluster, params.taskManagers * params.taskSlots)

            Status.appendTasks(flinkCluster, statusList)

            return Result(
                ResultStatus.SUCCESS,
                statusList
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't change tasks sequence", e)

            return Result(
                ResultStatus.FAILED,
                listOf()
            )
        }
    }

    private fun tryScalingCluster(flinkCluster: V1FlinkCluster, params: ClusterScaling): List<ClusterTask> {
        val clusterStatus = Status.getClusterStatus(flinkCluster)

        val bootstrapSpec = flinkCluster.spec?.bootstrap

        return if (bootstrapSpec == null) {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.taskManagers > 0) {
                        listOf(
                            ClusterTask.RescaleCluster,
                            ClusterTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.TerminatePods,
                            ClusterTask.SuspendCluster,
                            ClusterTask.ClusterHalted
                        )
                    }
                else -> listOf()
            }
        } else {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.taskManagers > 0) {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.CancelJob,
                            ClusterTask.RescaleCluster,
                            ClusterTask.StartingCluster,
//                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.CancelJob,
                            ClusterTask.TerminatePods,
                            ClusterTask.SuspendCluster,
                            ClusterTask.ClusterHalted
                        )
                    }
                else -> listOf()
            }
        }
    }
}