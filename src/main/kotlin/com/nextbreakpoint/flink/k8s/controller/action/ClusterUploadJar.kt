package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import org.apache.log4j.Logger
import java.io.File

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
            logger.error("Can't run JAR file", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}