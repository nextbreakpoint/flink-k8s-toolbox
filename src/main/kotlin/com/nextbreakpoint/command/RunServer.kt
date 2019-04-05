package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.FilnkSubmitVerticle
import com.nextbreakpoint.model.ServerConfig
import io.vertx.core.Launcher

class RunServer {
    fun run(serverConfig: ServerConfig) {
        System.setProperty("crypto.policy", "unlimited")
        System.setProperty("vertx.graphite.options.enabled", "true")
        System.setProperty("vertx.graphite.options.registryName", "exported")

        Launcher.main(
            arrayOf(
                "run",
                FilnkSubmitVerticle::class.java.canonicalName,
                "-conf",
                Gson().toJson(serverConfig)
            )
        )
    }
}
