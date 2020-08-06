package com.nextbreakpoint.flink.cli.factory

import com.nextbreakpoint.flink.common.ConnectionConfig
import io.vertx.core.net.JksOptions
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.client.WebClient

object WebClientDefaultFactory : WebClientFactory {
    override fun create(connectionConfig: ConnectionConfig) =
        createWebClient(
            host = connectionConfig.host,
            port = connectionConfig.port,
            keystorePath = connectionConfig.keystorePath,
            keystoreSecret = connectionConfig.keystoreSecret,
            truststorePath = connectionConfig.truststorePath,
            truststoreSecret = connectionConfig.truststoreSecret
        )

    private fun createWebClient(
        host: String = "localhost",
        port: Int,
        keystorePath: String?,
        keystoreSecret: String?,
        truststorePath: String?,
        truststoreSecret: String?
    ): WebClient {
        val webClientOptions = WebClientOptions()
            .setFollowRedirects(true)
            .setDefaultHost(host)
            .setDefaultPort(port)
//          .setLogActivity(true)

        if (keystorePath != null && truststorePath != null) {
            webClientOptions
                .setSsl(true)
                .setForceSni(false)
                .setVerifyHost(true)
                .setKeyStoreOptions(JksOptions().setPath(keystorePath).setPassword(keystoreSecret))
                .setTrustOptions(JksOptions().setPath(truststorePath).setPassword(truststoreSecret))
        }

        return WebClient.create(Vertx.vertx(), webClientOptions)
    }
}