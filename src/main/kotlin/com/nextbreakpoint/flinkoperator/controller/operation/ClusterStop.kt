package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.Status
import org.apache.log4j.Logger

class ClusterStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val cache: Cache) : Operation<StopOptions, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StopOptions): Result<List<ClusterTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val statusList = tryStoppingCluster(flinkCluster, params)

            if (statusList.isEmpty()) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return Result(
                    ResultStatus.AWAIT,
                    listOf()
                )
            }

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

    private fun tryStoppingCluster(flinkCluster: V1FlinkCluster, params: StopOptions): List<ClusterTask> {
        val clusterStatus = Status.getClusterStatus(flinkCluster)

        val bootstrapSpec = flinkCluster.spec?.bootstrap

        return if (bootstrapSpec == null) {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.deleteResources) {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.TerminatePods,
                            ClusterTask.DeleteResources,
                            ClusterTask.TerminatedCluster,
                            ClusterTask.ClusterHalted
                        )
                    } else {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.TerminatePods,
                            ClusterTask.SuspendCluster,
                            ClusterTask.ClusterHalted
                        )
                    }
                ClusterStatus.Suspended ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteResources,
                        ClusterTask.TerminatedCluster,
                        ClusterTask.ClusterHalted
                    )
                ClusterStatus.Failed ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteResources,
                        ClusterTask.TerminatedCluster,
                        ClusterTask.ClusterHalted
                    )
                else -> listOf()
            }
        } else {
            when (clusterStatus) {
                ClusterStatus.Running ->
                    if (params.deleteResources) {
                        if (params.withoutSavepoint) {
                            listOf(
                                ClusterTask.StoppingCluster,
                                ClusterTask.StopJob,
                                ClusterTask.TerminatePods,
                                ClusterTask.DeleteResources,
                                ClusterTask.TerminatedCluster,
                                ClusterTask.ClusterHalted
                            )
                        } else {
                            listOf(
                                ClusterTask.StoppingCluster,
                                ClusterTask.CancelJob,
                                ClusterTask.TerminatePods,
                                ClusterTask.DeleteResources,
                                ClusterTask.TerminatedCluster,
                                ClusterTask.ClusterHalted
                            )
                        }
                    } else {
                        if (params.withoutSavepoint) {
                            listOf(
                                ClusterTask.StoppingCluster,
                                ClusterTask.StopJob,
                                ClusterTask.TerminatePods,
                                ClusterTask.SuspendCluster,
                                ClusterTask.ClusterHalted
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
                    }
                ClusterStatus.Suspended ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteResources,
                        ClusterTask.TerminatedCluster,
                        ClusterTask.ClusterHalted
                    )
                ClusterStatus.Failed ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteResources,
                        ClusterTask.TerminatedCluster,
                        ClusterTask.ClusterHalted
                    )
                else -> listOf()
            }
        }
    }
}