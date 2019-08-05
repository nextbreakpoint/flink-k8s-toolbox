package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.common.ServerCommand
import com.nextbreakpoint.operator.OperatorConfig
import com.nextbreakpoint.operator.OperatorVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.impl.launcher.VertxCommandLauncher
import io.vertx.core.impl.launcher.VertxLifecycleHooks
import io.vertx.core.json.JsonObject
import io.vertx.micrometer.Label
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
//            System.setProperty("com.sun.management.jmxremote", "")
//            System.setProperty("vertx.metrics.options.enabled", "true")
//            System.setProperty("vertx.metrics.options.jmxEnabled", "true")
//            System.setProperty("vertx.prometheus.options.enabled", "true")

            dispatch(this, arrayOf("run", OperatorVerticle::class.java.canonicalName, "-conf", Gson().toJson(config)))

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
