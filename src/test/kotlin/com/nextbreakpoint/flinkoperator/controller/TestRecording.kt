package com.nextbreakpoint.flinkoperator.controller

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.util.Config
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class TestRecording {
    @Test
    @Disabled
    fun shouldRecord() {
        try {
            val options = WireMockConfiguration()

            options.httpsPort(9000)
                .notifier(ConsoleNotifier(true))
                .withRootDirectory("tmp")

            val wireMockServer = WireMockServer(options)

            try {
                wireMockServer.start()

//                wireMockServer.startRecording("https://kubernetes.docker.internal:6443")

                val client = Config.fromConfig("config")
                client.setApiKeyPrefix("Bearer")
                client.setApiKey("")
                client.isVerifyingSsl = false
                client.isDebugging = true

                val coreApi = CoreV1Api(client)

                coreApi.listPodForAllNamespaces(null, null, null, null, null, null, null, null)

//                wireMockServer.stopRecording()

                coreApi.listPodForAllNamespaces(null, null, null, null, null, null, null, null)
            } finally {
                wireMockServer.stop()
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}