package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flinkoperator.cli.UploadCommand
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.UploadOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import org.apache.log4j.Logger
import java.io.File

class UploadJAR : UploadCommand<UploadOptions> {
    companion object {
        private val logger = Logger.getLogger(UploadJAR::class.simpleName)
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, args: UploadOptions) {
        try {
            logger.info("Uploading JAR file ${args.jarPath}...")

            val address = KubernetesContext.findFlinkAddress(flinkOptions, namespace, clusterName)

            val result = FlinkContext.uploadJarCall(address, File(args.jarPath))

            if (result.status == JarUploadResponseBody.StatusEnum.SUCCESS) {
                logger.info("File ${args.jarPath} uploaded to ${result.filename}")
            } else {
                throw Exception("Failed to upload file ${args.jarPath}")
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
