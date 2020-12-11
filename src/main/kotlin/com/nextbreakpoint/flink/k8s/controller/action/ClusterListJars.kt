package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import java.util.logging.Level
import java.util.logging.Logger

class ClusterListJars(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<Void?, List<JarFileInfo>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterListJars::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: Void?): Result<List<JarFileInfo>> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val files = flinkClient.listJars(address).sortedByDescending { it.uploaded }

            Result(
                ResultStatus.OK,
                files
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't list JAR files", e)

            Result(
                ResultStatus.ERROR,
                listOf()
            )
        }
    }
}