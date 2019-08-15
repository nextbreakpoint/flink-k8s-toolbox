package com.nextbreakpoint.flinkoperator.cli.command

import com.google.gson.Gson
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
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

            val response = flinkApi.uploadJarCall(File(args.jarPath), null, null).execute()

            if (response.isSuccessful) {
                response.body().use {
                    val result = Gson().fromJson(it.source().readUtf8Line(), JarUploadResponseBody::class.java)
                    if (result.status.name.equals("SUCCESS")) {
                        logger.info("File ${args.jarPath} uploaded to ${result.filename}")
                    } else {
                        throw RuntimeException("Failed to upload file ${args.jarPath}")
                    }
                }
            } else {
                throw RuntimeException("Failed to upload file ${args.jarPath}")
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
