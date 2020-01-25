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

            var count = 0;

            while (count < 5) {
                try {
                    val uploadResult = FlinkClient.uploadJarCall(address, File(args.jarPath))

                    if (uploadResult.status == JarUploadResponseBody.StatusEnum.SUCCESS) {
                        logger.info("File ${args.jarPath} uploaded to ${uploadResult.filename}")

                        break;
                    } else {
                        logger.warn("Failed to upload file. Retrying...")
                    }
                } catch (e : Exception) {
                    logger.warn("Failed to upload file. Retrying...", e)
                }

                Thread.sleep(5000)

                count += 1
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
                logger.info("Resuming from savepoint $parallelism")
            }

            count = 0;

            while (count < 5) {
                try {
                    FlinkClient.runJar(address, jarFile, className, parallelism, savepointPath, arguments)
                } catch (e : Exception) {
                    logger.warn("Failed to run job. Retrying...", e)
                }

                Thread.sleep(5000)

                count += 1
            }

            val jobs = FlinkClient.listRunningJobs(address)

            if (jobs.isEmpty()) {
                throw Exception("Can't find running job")
            }

            logger.info("Job started")
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
