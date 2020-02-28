package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class ClusterStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val adapter: CacheAdapter) : Operation<StopOptions, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StopOptions): OperationResult<List<ClusterTask>> {
        try {
            val statusList = tryStoppingCluster(adapter.getBootstrap(), adapter.getClusterStatus(), params)

            if (statusList.isEmpty()) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return OperationResult(
                    OperationStatus.RETRY,
                    listOf()
                )
            }

            adapter.appendTasks(statusList)

            return OperationResult(
                OperationStatus.COMPLETED,
                statusList
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't change tasks sequence", e)

            return OperationResult(
                OperationStatus.FAILED,
                listOf()
            )
        }
    }

    private fun tryStoppingCluster(bootstrapSpec: V1BootstrapSpec?, clusterStatus: ClusterStatus, params: StopOptions): List<ClusterTask> {
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
                ClusterStatus.Terminated ->
                    listOf(
                        ClusterTask.ClusterHalted
                    )
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
                                ClusterTask.DeleteBootstrapJob,
                                ClusterTask.DeleteResources,
                                ClusterTask.TerminatedCluster,
                                ClusterTask.ClusterHalted
                            )
                        } else {
                            listOf(
                                ClusterTask.StoppingCluster,
                                ClusterTask.CancelJob,
                                ClusterTask.TerminatePods,
                                ClusterTask.DeleteBootstrapJob,
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
                                ClusterTask.DeleteBootstrapJob,
                                ClusterTask.SuspendCluster,
                                ClusterTask.ClusterHalted
                            )
                        } else {
                            listOf(
                                ClusterTask.StoppingCluster,
                                ClusterTask.CancelJob,
                                ClusterTask.TerminatePods,
                                ClusterTask.DeleteBootstrapJob,
                                ClusterTask.SuspendCluster,
                                ClusterTask.ClusterHalted
                            )
                        }
                    }
                ClusterStatus.Terminated ->
                    listOf(
                        ClusterTask.ClusterHalted
                    )
                ClusterStatus.Suspended ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteBootstrapJob,
                        ClusterTask.DeleteResources,
                        ClusterTask.TerminatedCluster,
                        ClusterTask.ClusterHalted
                    )
                ClusterStatus.Failed ->
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteBootstrapJob,
                        ClusterTask.DeleteResources,
                        ClusterTask.TerminatedCluster,
                        ClusterTask.ClusterHalted
                    )
                else -> listOf()
            }
        }
    }
}