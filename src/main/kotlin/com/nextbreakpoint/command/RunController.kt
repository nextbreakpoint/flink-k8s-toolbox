package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.ControllerVerticle
import com.nextbreakpoint.model.ControllerConfig
import io.vertx.core.Launcher
import org.apache.log4j.Logger

class RunController {
    companion object {
        val logger = Logger.getLogger(RunController::class.simpleName)
    }

    fun run(controllerConfig: ControllerConfig) {
        try {
            System.setProperty("crypto.policy", "unlimited")
            System.setProperty("vertx.graphite.options.enabled", "true")
            System.setProperty("vertx.graphite.options.registryName", "exported")
            logger.info("Launching controller...")
            Launcher.main(arrayOf("run", ControllerVerticle::class.java.canonicalName, "-conf", Gson().toJson(controllerConfig)))
            while (true) {
                if (Thread.interrupted()) {
                    break
                }
            }
        } catch (e: Exception) {
            logger.error("An error occurred while launching the server", e)
            throw RuntimeException(e)
        }
    }
}
