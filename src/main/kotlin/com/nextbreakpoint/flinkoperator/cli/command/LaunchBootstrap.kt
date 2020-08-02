package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flinkoperator.cli.LaunchCommand
import com.nextbreakpoint.flinkoperator.common.model.BootstrapOptions
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import org.apache.log4j.Logger
import java.io.File

class LaunchBootstrap : LaunchCommand<BootstrapOptions> {
    companion object {
        private val logger = Logger.getLogger(LaunchBootstrap::class.simpleName)
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, args: BootstrapOptions) {
        try {
            logger.info("Uploading JAR file ${args.jarPath}...")

            val address = KubeClient.findFlinkAddress(flinkOptions, namespace, args.clusterName)

            var count = 0;

            val maxAttempts = 12

            while (count < maxAttempts && !uploadFile(address, args)) {
                Thread.sleep(10000)
                count += 1
            }

            if (count >= maxAttempts) {
                throw Exception("Failed to upload JAR file. Max attempts reached")
            }

            val files = FlinkClient.listJars(address)

            val jarFile = files.maxBy { it.uploaded } ?: throw Exception("Can't find any JAR file")

            val savepointPath = args.savepointPath
            val parallelism = args.parallelism
            val className = args.className
            val arguments = args.arguments

            logger.info("Main class is $className")

            logger.info("Running job with parallelism $parallelism")

            if (savepointPath != null) {
                logger.info("Resuming from savepoint $savepointPath")
            }

            FlinkClient.runJar(address, jarFile, className, parallelism, savepointPath, arguments)

            logger.info("Job started")
        } catch (e: Exception) {
            logger.error("An error occurred while starting job", e)
        }
    }

    private fun uploadFile(address: FlinkAddress, args: BootstrapOptions): Boolean {
        try {
            val uploadResult = FlinkClient.uploadJarCall(address, File(args.jarPath))

            if (uploadResult.status == JarUploadResponseBody.StatusEnum.SUCCESS) {
                logger.info("File ${args.jarPath} uploaded to ${uploadResult.filename}")

                return true
            } else {
                logger.warn("Failed to upload file. Retrying...")
            }
        } catch (e: Exception) {
            logger.warn("Failed to upload file. Retrying...", e)
        }
        return false
    }
}
