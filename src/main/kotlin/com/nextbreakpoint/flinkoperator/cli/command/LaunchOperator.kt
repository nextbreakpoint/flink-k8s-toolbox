package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.LaunchCommand
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorOptions
import com.nextbreakpoint.flinkoperator.controller.MonitoringVerticle
import com.nextbreakpoint.flinkoperator.controller.OperatorVerticle
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

class LaunchOperator : VertxCommandLauncher(), VertxLifecycleHooks, LaunchCommand<OperatorOptions> {
    companion object {
        private val logger = Logger.getLogger(LaunchOperator::class.simpleName)
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, args: OperatorOptions) {
        try {
            logger.info("Launching operator...")

            val jsonObject = JsonObject()
                .put("namespace", namespace)
                .put("flink_hostname", flinkOptions.hostname)
                .put("port_forward", flinkOptions.portForward)
                .put("use_node_port", flinkOptions.useNodePort)
                .put("server_keystore_path", args.keystorePath)
                .put("server_keystore_secret", args.keystoreSecret)
                .put("server_truststore_path", args.truststorePath)
                .put("server_truststore_secret", args.truststoreSecret)

            dispatch(this, arrayOf("run", OperatorVerticle::class.java.canonicalName, "-conf", jsonObject.toString()))

            dispatch(this, arrayOf("run", MonitoringVerticle::class.java.canonicalName))

            waitUntilInterrupted()
        } catch (e: InterruptedException) {
            logger.error("Terminating operator...")
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    private fun waitUntilInterrupted() {
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
