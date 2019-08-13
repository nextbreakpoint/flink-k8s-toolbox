package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.ServerCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorVerticle
import com.nextbreakpoint.flinkoperator.common.model.OperatorConfig
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.impl.launcher.VertxCommandLauncher
import io.vertx.core.impl.launcher.VertxLifecycleHooks
import io.vertx.core.json.JsonObject
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

class LaunchOperator : VertxCommandLauncher(), VertxLifecycleHooks,
    ServerCommand<OperatorConfig> {
    companion object {
        private val logger = Logger.getLogger(LaunchOperator::class.simpleName)
    }

    override fun run(args: OperatorConfig) {
        try {
            logger.info("Launching operator...")

            val jsonObject = JsonObject()
                .put("namespace", args.namespace)
                .put("flink_hostname", args.flinkHostname)
                .put("port_forward", args.portForward)
                .put("use_node_port", args.useNodePort)
                .put("server_keystore_path", args.keystorePath)
                .put("server_keystore_secret", args.keystoreSecret)
                .put("server_truststore_path", args.truststorePath)
                .put("server_truststore_secret", args.truststoreSecret)

            dispatch(this, arrayOf("run", OperatorVerticle::class.java.canonicalName, "-conf", jsonObject.toString()))

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
                    .setStartEmbeddedServer(true)
                    .setEmbeddedServerEndpoint("/metrics")
                    .setEmbeddedServerOptions(
                        HttpServerOptions().setSsl(false).setPort(8080)
                    )
            )
            .setEnabled(true)
            .setRegistryName("flink-operator")

        vertxOptions?.workerPoolSize = 4
        vertxOptions?.eventLoopPoolSize = 1
        vertxOptions?.maxWorkerExecuteTime = 20
        vertxOptions?.maxWorkerExecuteTimeUnit = TimeUnit.SECONDS
        vertxOptions?.warningExceptionTime = 10
        vertxOptions?.warningExceptionTimeUnit = TimeUnit.SECONDS
        vertxOptions?.maxEventLoopExecuteTime = 20
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
