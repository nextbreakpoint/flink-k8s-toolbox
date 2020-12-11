package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus.StatusEnum.CANCELED
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus.StatusEnum.FAILED
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus.StatusEnum.FINISHED
import java.util.logging.Level
import java.util.logging.Logger

class ClusterStopJobs(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<Set<String>, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterStopJobs::class.simpleName)
        private val finalStatuses = setOf(FAILED, FINISHED, CANCELED)
    }

    override fun execute(namespace: String, clusterName: String, params: Set<String>): Result<Boolean> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val jobs = flinkClient.listJobs(address, setOf()).filter { !params.contains(it.key) }.toMap()

            val completedJobs = jobs.filter { finalStatuses.contains(it.value) }

            val runningJobs = jobs.filter { !completedJobs.contains(it.key) }

            runningJobs.forEach {
                logger.info("Stopping job ${it.key} (${it.value.name})")
            }

            if (runningJobs.isNotEmpty()) {
                flinkClient.terminateJobs(address, runningJobs.keys.toList())

                return Result(
                    ResultStatus.OK,
                    false
                )
            } else {
                return Result(
                    ResultStatus.OK,
                    true
                )
            }
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't stop jobs", e)

            return Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}