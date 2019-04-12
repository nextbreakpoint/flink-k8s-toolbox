package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.FilnkSubmitVerticle
import com.nextbreakpoint.model.ServerConfig
import io.vertx.core.Launcher
import org.apache.log4j.Logger

class RunServer {
    companion object {
        val logger = Logger.getLogger(RunServer::class.simpleName)
    }

    fun run(serverConfig: ServerConfig) {
        try {
            System.setProperty("crypto.policy", "unlimited")
            System.setProperty("vertx.graphite.options.enabled", "true")
            System.setProperty("vertx.graphite.options.registryName", "exported")
            logger.info("Launching FlinkSubmit server...")
            Launcher.main(arrayOf("run", FilnkSubmitVerticle::class.java.canonicalName, "-conf", Gson().toJson(serverConfig)))
        } catch (e: Exception) {
            logger.error("An error occurred while launching the server", e)
            throw RuntimeException(e)
        }
    }
}
