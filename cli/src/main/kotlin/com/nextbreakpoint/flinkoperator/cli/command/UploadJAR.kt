package com.nextbreakpoint.flinkoperator.cli.command

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.cli.UploadCommand
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.UploadOptions
import org.apache.log4j.Logger
import java.io.File

class UploadJAR : UploadCommand<UploadOptions> {
    companion object {
        private val logger = Logger.getLogger(UploadJAR::class.simpleName)
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, args: UploadOptions) {
        try {
            logger.info("Uploading JAR file ${args.jarPath}...")

            val flinkApi = FlinkServerUtils.find(flinkOptions, namespace, clusterName)

            val result = flinkApi.uploadJar(File(args.jarPath))

            if (result.status.name.equals("SUCCESS")) {
                logger.info("JAR file uploaded: ${Gson().toJson(result)}")
            } else {
                throw RuntimeException("Failed to upload JAR file ${args.jarPath} to cluster $namespace/$clusterName")
            }
        } catch (e: Exception) {
            logger.error("An error occurred while uploading the file ${args.jarPath} to cluster $namespace/$clusterName", e)
            throw RuntimeException(e)
        }
    }
}
