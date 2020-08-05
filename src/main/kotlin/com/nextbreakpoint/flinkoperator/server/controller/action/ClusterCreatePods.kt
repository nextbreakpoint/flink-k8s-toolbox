package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.PodReplicas
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterCreatePods(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<PodReplicas, Set<String>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreatePods::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: PodReplicas): Result<Set<String>> {
        return try {
            logger.debug("[name=${clusterSelector.name}] Creating pods...")

            val sequence = (1 .. params.replicas).asSequence()

            val output = sequence.map {
                kubeClient.createPod(clusterSelector, params.pod)
            }.map {
                it.metadata.name
            }.toSet()

            Result(
                ResultStatus.OK,
                output
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create pods", e)

            Result(
                ResultStatus.ERROR,
                setOf()
            )
        }
    }
}