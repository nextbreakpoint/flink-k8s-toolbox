package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.Status
import org.apache.log4j.Logger

class ClusterCheckpointing(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: Cache) : Operation<Void?, List<ClusterTask>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterCheckpointing::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<List<ClusterTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            if (flinkCluster.spec?.bootstrap == null) {
                logger.info("Job not defined for cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    listOf()
                )
            }

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

            val clusterStatus = Status.getClusterStatus(flinkCluster)

            if (clusterStatus != ClusterStatus.Running) {
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

            val statusList = listOf(
                ClusterTask.CreatingSavepoint,
                ClusterTask.TriggerSavepoint,
                ClusterTask.ClusterRunning
            )

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
}