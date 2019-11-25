package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class ClusterScale(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val adapter: CacheAdapter) : Operation<ClusterScaling, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterScaling::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterScaling): Result<List<ClusterTask>> {
        try {
            val statusList = tryScalingCluster(adapter.getBootstrap(), adapter.getClusterStatus(), params)

            if (statusList.isEmpty()) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return Result(
                    ResultStatus.AWAIT,
                    listOf()
                )
            }

            adapter.setTaskManagers(params.taskManagers)
            adapter.setTaskSlots(params.taskSlots)
            adapter.setJobParallelism(params.taskManagers * params.taskSlots)

            adapter.appendTasks(statusList)

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

    private fun tryScalingCluster(bootstrapSpec: V1BootstrapSpec?, clusterStatus: ClusterStatus, params: ClusterScaling): List<ClusterTask> {
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
                            ClusterTask.CreateBootstrapJob,
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