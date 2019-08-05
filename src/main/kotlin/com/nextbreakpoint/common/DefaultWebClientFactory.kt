package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Address
import io.vertx.core.net.JksOptions
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.client.WebClient

object DefaultWebClientFactory : WebClientFactory {
    override fun create(params: Address) = createWebClient(
        host = params.host,
        port = params.port,
        keystorePath = params.keystorePath,
        keystoreSecret = params.keystoreSecret
    )

    private fun createWebClient(host: String = "localhost", port: Int, keystorePath: String, keystoreSecret: String): WebClient {
        val clientOptions = WebClientOptions()
//        clientOptions.logActivity = true
        clientOptions.isFollowRedirects = true
        clientOptions.defaultHost = host
        clientOptions.defaultPort = port
        val keystoreOptions = JksOptions().setPath(keystorePath).setPassword(keystoreSecret)
        clientOptions.setSsl(true).setKeyStoreOptions(keystoreOptions)
        return WebClient.create(Vertx.vertx(), clientOptions)
    }
}