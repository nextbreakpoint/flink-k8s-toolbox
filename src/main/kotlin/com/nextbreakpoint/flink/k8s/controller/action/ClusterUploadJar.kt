package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import java.io.File
import java.util.logging.Level
import java.util.logging.Logger

class ClusterUploadJar(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<File, JarUploadResponseBody?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterUploadJar::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: File): Result<JarUploadResponseBody?> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val result = flinkClient.uploadJar(address, params)

            Result(
                ResultStatus.OK,
                result
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't run JAR file", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}