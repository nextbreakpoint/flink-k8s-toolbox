package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScale
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.PodReplicas
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class ClusterCreatePods(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<PodReplicas, Set<String>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreatePods::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: PodReplicas): OperationResult<Set<String>> {
        return try {
            logger.debug("[name=${clusterSelector.name}] Creating pods...")

            val sequence = (1 .. params.replicas).asSequence()

            val output = sequence.map {
                kubeClient.createPod(clusterSelector, params.pod)
            }.map {
                it.metadata.name
            }.toSet()

            OperationResult(
                OperationStatus.OK,
                output
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create pods", e)

            OperationResult(
                OperationStatus.ERROR,
                setOf()
            )
        }
    }
}