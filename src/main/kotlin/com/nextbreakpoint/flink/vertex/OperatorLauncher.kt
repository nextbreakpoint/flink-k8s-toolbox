package com.nextbreakpoint.flink.vertex

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.OperatorOptions
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.impl.launcher.VertxCommandLauncher
import io.vertx.core.impl.launcher.VertxLifecycleHooks
import io.vertx.core.json.JsonObject
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class OperatorLauncher(
    private val flinkOptions: FlinkOptions,
    private val namespace: String,
    private val options: OperatorOptions
) : VertxCommandLauncher(), VertxLifecycleHooks {
    companion object {
        private val logger = Logger.getLogger(OperatorLauncher::class.simpleName)
    }

    fun launch() {
        try {
            logger.info("Launching operator...")

            val jsonObject = JsonObject()
                .put("namespace", namespace)
                .put("flink_hostname", flinkOptions.hostname)
                .put("port_forward", flinkOptions.portForward)
                .put("use_node_port", flinkOptions.useNodePort)
                .put("server_keystore_path", options.keystorePath)
                .put("server_keystore_secret", options.keystoreSecret)
                .put("server_truststore_path", options.truststorePath)
                .put("server_truststore_secret", options.truststoreSecret)
                .put("polling_interval", options.pollingInterval)
                .put("task_timeout", options.taskTimeout)
                .put("dry_run", options.dryRun)

            dispatch(this, arrayOf("run", OperatorVerticle::class.java.canonicalName, "-conf", jsonObject.toString()))

            dispatch(this, arrayOf("run", MonitoringVerticle::class.java.canonicalName))
        } catch (e: InterruptedException) {
            logger.error("Terminating operator...")
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    fun waitUntilInterrupted() {
        try {
            while (true) {
                if (Thread.interrupted()) {
                    break
                }
                Thread.sleep(60000)
            }
        } catch (e: InterruptedException) {
        }
    }

    override fun handleDeployFailed(vertx: Vertx?, mainVerticle: String?, deploymentOptions: DeploymentOptions?, error: Throwable?) {
    }

    override fun beforeStartingVertx(vertxOptions: VertxOptions?) {
        vertxOptions?.metricsOptions =
            MicrometerMetricsOptions().setPrometheusOptions(
                VertxPrometheusOptions()
                    .setEnabled(true)
//                    .setStartEmbeddedServer(true)
//                    .setEmbeddedServerEndpoint("/metrics")
//                    .setEmbeddedServerOptions(
//                        HttpServerOptions().setSsl(false).setPort(8080)
//                    )
            )
            .setEnabled(true)
            .setRegistryName("flink-operator")

        vertxOptions?.workerPoolSize = 4
        vertxOptions?.eventLoopPoolSize = 1
        vertxOptions?.maxWorkerExecuteTime = 30
        vertxOptions?.maxWorkerExecuteTimeUnit = TimeUnit.SECONDS
        vertxOptions?.warningExceptionTime = 20
        vertxOptions?.warningExceptionTimeUnit = TimeUnit.SECONDS
        vertxOptions?.maxEventLoopExecuteTime = 30
        vertxOptions?.maxEventLoopExecuteTimeUnit = TimeUnit.SECONDS
    }

    override fun afterStoppingVertx() {
    }

    override fun afterConfigParsed(p0: JsonObject?) {
    }

    override fun afterStartingVertx(vertx: Vertx?) {
    }

    override fun beforeStoppingVertx(vertx: Vertx?) {
    }

    override fun beforeDeployingVerticle(deploymentOptions: DeploymentOptions?) {
    }
}
