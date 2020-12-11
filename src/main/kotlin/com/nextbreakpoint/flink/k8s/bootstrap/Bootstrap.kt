package com.nextbreakpoint.flink.k8s.bootstrap

import com.nextbreakpoint.flink.common.BootstrapOptions
import com.nextbreakpoint.flink.common.RunJarOptions
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import java.io.File
import java.util.logging.Level
import java.util.logging.Logger

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
            val file = File(options.jarPath)

            logger.info("Uploading JAR file: ${file.name} [${file.absolutePath}]...")

            uploadJarFile(file)

            val listJarsResult = controller.listJars(namespace, options.clusterName)

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

            val runJarResult = controller.runJar(namespace, options.clusterName, runJarOptions)

            if (!runJarResult.isSuccessful()) {
                throw Exception("Failed to run JAR file")
            }

            val name = "${options.clusterName}-${options.jobName}"

            val flinkJob = controller.getFlinkJob(namespace, name)

            FlinkJobStatus.setJobId(flinkJob, runJarResult.output)

            controller.updateStatus(namespace, name, flinkJob)

            logger.info("Job started (jobID = ${runJarResult.output})")
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "An error occurred while booting job", e)
            throw e
        }
    }

    private fun uploadJarFile(file: File): Boolean {
        try {
            val uploadResult = controller.uploadJar(namespace, options.clusterName, file)

            if (!uploadResult.isSuccessful()) {
                throw Exception("Failed to upload JAR file")
            }

            if (uploadResult.output?.status != JarUploadResponseBody.StatusEnum.SUCCESS) {
                throw Exception("Failed to upload JAR file")
            }

            logger.info("File ${file.name} uploaded to ${uploadResult.output.filename}")

            return true
        } catch (e: Exception) {
            logger.log(Level.WARNING, "Failed to upload file. Retrying...", e)

            return false
        }
    }
}