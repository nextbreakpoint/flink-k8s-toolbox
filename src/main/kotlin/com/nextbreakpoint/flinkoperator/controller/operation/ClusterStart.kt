package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.Status
import org.apache.log4j.Logger

class ClusterStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val cache: Cache) : Operation<StartOptions, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StartOptions): Result<List<ClusterTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val operatorStatus = Status.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus != TaskStatus.Idle) {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        Status.getCurrentTask(
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
                        Status.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            Status.appendTasks(flinkCluster, statusList)

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

    private fun tryStartingCluster(flinkCluster: V1FlinkCluster, params: StartOptions): List<ClusterTask> {
        val clusterStatus = Status.getClusterStatus(flinkCluster)

        val bootstrapSpec = flinkCluster.spec?.bootstrap

        return if (bootstrapSpec == null) {
            when (clusterStatus) {
                ClusterStatus.Terminated ->
                    listOf(
                        ClusterTask.StartingCluster,
                        ClusterTask.CreateResources,
                        ClusterTask.ClusterRunning
                    )
                ClusterStatus.Suspended ->
                    listOf(
                        ClusterTask.StartingCluster,
                        ClusterTask.RestartPods,
                        ClusterTask.ClusterRunning
                    )
                ClusterStatus.Failed ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteResources,
                        ClusterTask.StartingCluster,
                        ClusterTask.CreateResources,
                        ClusterTask.ClusterRunning
                    )
                else -> listOf()
            }
        } else {
            when (clusterStatus) {
                ClusterStatus.Terminated ->
                    if (params.withoutSavepoint) {
                        listOf(
                            ClusterTask.StartingCluster,
                            ClusterTask.CreateResources,
                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.EraseSavepoint,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            ClusterTask.StartingCluster,
                            ClusterTask.CreateResources,
                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    }
                ClusterStatus.Suspended ->
                    if (params.withoutSavepoint) {
                        listOf(
                            ClusterTask.StartingCluster,
                            ClusterTask.RestartPods,
                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.EraseSavepoint,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            ClusterTask.StartingCluster,
                            ClusterTask.RestartPods,
                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    }
                ClusterStatus.Failed ->
                    if (params.withoutSavepoint) {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.TerminatePods,
                            ClusterTask.DeleteResources,
                            ClusterTask.StartingCluster,
                            ClusterTask.CreateResources,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.EraseSavepoint,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    } else {
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.DeleteBootstrapJob,
                            ClusterTask.TerminatePods,
                            ClusterTask.DeleteResources,
                            ClusterTask.StartingCluster,
                            ClusterTask.CreateResources,
                            ClusterTask.CreateBootstrapJob,
                            ClusterTask.StartJob,
                            ClusterTask.ClusterRunning
                        )
                    }
                else -> listOf()
            }
        }
    }
}