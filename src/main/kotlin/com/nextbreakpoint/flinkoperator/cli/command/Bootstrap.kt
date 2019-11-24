package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flinkoperator.cli.BootstrapCommand
import com.nextbreakpoint.flinkoperator.common.model.BootstrapOptions
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import org.apache.log4j.Logger
import java.io.File

class Bootstrap : BootstrapCommand<BootstrapOptions> {
    companion object {
        private val logger = Logger.getLogger(Bootstrap::class.simpleName)
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, args: BootstrapOptions) {
        try {
            logger.info("Uploading JAR file ${args.jarPath}...")

            val address = KubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val uploadResult = FlinkClient.uploadJarCall(address, File(args.jarPath))

            if (uploadResult.status != JarUploadResponseBody.StatusEnum.SUCCESS) {
                throw Exception("Failed to upload file ${args.jarPath}")
            }

            logger.info("File ${args.jarPath} uploaded to ${uploadResult.filename}")

            val files = FlinkClient.listJars(address)

            val jarFile = files.maxBy { it.uploaded } ?: throw Exception("Can't find any JAR file")

            val savepointPath = args.savepointPath
            val parallelism = args.parallelism
            val className = args.className
            val arguments = args.arguments

            logger.info("Main class is $className")

            logger.info("Running job with parallelism $parallelism")

            if (savepointPath != null) {
                logger.info("Resuming from savepoint $parallelism")
            }

            FlinkClient.runJar(address, jarFile, className, parallelism, savepointPath, arguments)

            logger.info("Job started")
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
