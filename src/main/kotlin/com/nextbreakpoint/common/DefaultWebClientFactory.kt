package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ConnectionConfig
import io.vertx.core.net.JksOptions
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.client.WebClient
import org.apache.log4j.Logger

object DefaultWebClientFactory : WebClientFactory {
    private val logger: Logger = Logger.getLogger(DefaultWebClientFactory::class.simpleName)

    override fun create(params: ConnectionConfig) = createWebClient(
        host = params.host,
        port = params.port,
        keystorePath = params.keystorePath,
        keystoreSecret = params.keystoreSecret,
        truststorePath = params.truststorePath,
        truststoreSecret = params.truststoreSecret
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
            logger.info("Enabling HTTPS with host verification")

            webClientOptions
                .setSsl(true)
                .setForceSni(false)
                .setVerifyHost(true)
                .setKeyStoreOptions(JksOptions().setPath(keystorePath).setPassword(keystoreSecret))
                .setTrustOptions(JksOptions().setPath(truststorePath).setPassword(truststoreSecret))
        } else {
            logger.warn("HTTPS not enabled!")
        }

        return WebClient.create(Vertx.vertx(), webClientOptions)
    }
}