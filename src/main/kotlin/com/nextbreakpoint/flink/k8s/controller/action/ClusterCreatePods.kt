package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.PodReplicas
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger
import java.lang.RuntimeException

class ClusterCreatePods(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<PodReplicas, Set<String>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreatePods::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: PodReplicas): Result<Set<String>> {
        return try {
            val sequence = (1 .. params.replicas).asSequence()

            val output = sequence.map {
                kubeClient.createPod(clusterSelector, params.pod)
            }.map {
                it.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            }.toSet()

            Result(
                ResultStatus.OK,
                output
            )
        } catch (e : Exception) {
            logger.error("Can't create pods", e)

            Result(
                ResultStatus.ERROR,
                setOf()
            )
        }
    }
}