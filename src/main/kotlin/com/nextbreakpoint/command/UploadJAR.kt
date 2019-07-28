package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.UploadCommand
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.UploadOptions
import org.apache.log4j.Logger
import java.io.File

class UploadJAR : UploadCommand<UploadOptions> {
    companion object {
        private val logger = Logger.getLogger(UploadJAR::class.simpleName)
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, params: UploadOptions) {
        try {
            logger.info("Uploading JAR file ${params.jarPath}...")

            val flinkApi = Flink.find(flinkOptions, namespace, clusterName)

            val result = flinkApi.uploadJar(File(params.jarPath))

            if (result.status.name.equals("SUCCESS")) {
                logger.info("JAR file uploaded: ${Gson().toJson(result)}")
            } else {
                throw RuntimeException("Failed to upload JAR file ${params.jarPath} to cluster $namespace/$clusterName")
            }
        } catch (e: Exception) {
            logger.error("An error occurred while uploading the file ${params.jarPath} to cluster $namespace/$clusterName", e)
            throw RuntimeException(e)
        }
    }
}
