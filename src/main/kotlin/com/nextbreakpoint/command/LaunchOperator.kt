package com.nextbreakpoint.command

import com.nextbreakpoint.common.ServerCommand
import com.nextbreakpoint.operator.OperatorConfig
import com.nextbreakpoint.operator.OperatorVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.impl.launcher.VertxCommandLauncher
import io.vertx.core.impl.launcher.VertxLifecycleHooks
import io.vertx.core.json.JsonObject
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import org.apache.log4j.Logger

class LaunchOperator : VertxCommandLauncher(), VertxLifecycleHooks, ServerCommand<OperatorConfig> {
    companion object {
        private val logger = Logger.getLogger(LaunchOperator::class.simpleName)
    }

    override fun run(config: OperatorConfig) {
        try {
            logger.info("Launching operator...")

            System.setProperty("crypto.policy", "unlimited")

            val jsonObject = JsonObject()
                .put("namespace", config.namespace)
                .put("flink_hostname", config.flinkHostname)
                .put("port_forward", config.portForward)
                .put("use_node_port", config.useNodePort)
                .put("savepoint_interval", config.savepointInterval)
                .put("server_keystore_path", config.keystorePath)
                .put("server_keystore_secret", config.keystoreSecret)
                .put("server_truststore_path", config.truststorePath)
                .put("server_truststore_secret", config.truststoreSecret)

            dispatch(this, arrayOf("run", OperatorVerticle::class.java.canonicalName, "-conf", jsonObject.toString()))

            waitUntilInterrupted()
        } catch (e: InterruptedException) {
            logger.error("Terminating...")
        } catch (e: Exception) {
            logger.error("An error occurred while launching the operator", e)
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
                VertxPrometheusOptions().setEnabled(true)
            )
            .setEnabled(true)
            .setRegistryName("flink-operator")
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
