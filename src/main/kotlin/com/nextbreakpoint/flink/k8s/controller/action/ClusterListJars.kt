package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import org.apache.log4j.Logger

class ClusterListJars(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, List<JarFileInfo>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterListJars::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: Void?): Result<List<JarFileInfo>> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val files = flinkClient.listJars(address).sortedByDescending { it.uploaded }

            Result(
                ResultStatus.OK,
                files
            )
        } catch (e : Exception) {
            logger.error("Can't list JAR files", e)

            Result(
                ResultStatus.ERROR,
                listOf()
            )
        }
    }
}