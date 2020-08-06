package com.nextbreakpoint.flink.k8s.bootstrap

import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flink.common.BootstrapOptions
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.RunJarOptions
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import org.apache.log4j.Logger
import java.io.File

class Bootstrap(
    private val controller: Controller,
    private val namespace: String,
    private val options: BootstrapOptions
) {
    companion object {
        private val logger = Logger.getLogger(Bootstrap::class.simpleName)
    }

    fun run() {
        try {
            val clusterSelector = ResourceSelector(namespace = namespace, name = options.clusterName, uid = "")

            val file = File(options.jarPath)

            logger.info("Uploading JAR file: ${file.name} [${file.absolutePath}]...")

            uploadJarFile(clusterSelector, 3, file)

            val listJarsResult = controller.listJars(clusterSelector)

            if (!listJarsResult.isSuccessful()) {
                throw Exception("Failed to list JAR files")
            }

            val jarFile = listJarsResult.output
                .filter { it.name == file.name }
                .maxByOrNull { it.uploaded } ?: throw Exception("Can't find any JAR file")

            logger.info("Main class is ${options.className}")

            logger.info("Job parallelism is ${options.parallelism}")

            if (options.savepointPath != null) {
                logger.info("Resume job from savepoint ${options.savepointPath}")
            }

            // From the instant when the job is submitted to the instant when the job id is saved into the FlinkJob status
            // there is a chance that the bootstrap job is killed. The supervisor needs to check for missing job id, and
            // eventually restart the bootstrap job
            val runJarOptions = RunJarOptions(jarFile.id, options.className, options.parallelism, options.savepointPath, options.arguments)

            val runJarResult = controller.runJar(clusterSelector, runJarOptions)

            if (!runJarResult.isSuccessful()) {
                throw Exception("Failed to run JAR file")
            }

            val jobSelector = ResourceSelector(namespace = namespace, name = options.jobName, uid = "")

            val flinkJob = controller.getFlinkJob(jobSelector)

            FlinkJobStatus.setJobId(flinkJob, runJarResult.output)

            controller.updateStatus(jobSelector, flinkJob)

            logger.info("Job started (jobID = ${runJarResult.output})")
        } catch (e: Exception) {
            logger.error("An error occurred while booting job", e)
        }
    }

    private fun uploadJarFile(clusterSelector: ResourceSelector, maxAttempts: Int, file: File) {
        var count = 0;

        while (count < maxAttempts && !uploadJarFile(clusterSelector, file)) {
            Thread.sleep(10000)
            count += 1
        }

        if (count >= maxAttempts) {
            throw Exception("Failed to upload JAR file. Max attempts reached")
        }
    }

    private fun uploadJarFile(clusterSelector: ResourceSelector, file: File): Boolean {
        try {
            val uploadResult = controller.uploadJar(clusterSelector, file)

            if (!uploadResult.isSuccessful()) {
                throw Exception("Failed to upload JAR file")
            }

            if (uploadResult.output?.status != JarUploadResponseBody.StatusEnum.SUCCESS) {
                throw Exception("Failed to upload JAR file")
            }

            logger.info("File ${file.name} uploaded to ${uploadResult.output.filename}")

            return true
        } catch (e: Exception) {
            logger.warn("Failed to upload file. Retrying...", e)

            return false
        }
    }
}