package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.common.ServerCommand
import com.nextbreakpoint.operator.OperatorConfig
import com.nextbreakpoint.operator.OperatorVerticle
import io.vertx.core.Launcher
import org.apache.log4j.Logger

class LaunchOperator : ServerCommand<OperatorConfig> {
    companion object {
        val logger = Logger.getLogger(LaunchOperator::class.simpleName)
    }

    override fun run(config: OperatorConfig) {
        try {
            logger.info("Launching operator...")

            System.setProperty("crypto.policy", "unlimited")
            System.setProperty("vertx.graphite.options.enabled", "true")
            System.setProperty("vertx.graphite.options.registryName", "exported")

            Launcher.main(arrayOf("run", OperatorVerticle::class.java.canonicalName, "-conf", Gson().toJson(config)))

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
}
