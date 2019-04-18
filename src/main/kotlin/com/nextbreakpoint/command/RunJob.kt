package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.RunJobConfig
import org.apache.log4j.Logger

class RunJob {
    companion object {
        val logger = Logger.getLogger(RunJob::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, runJobConfig: RunJobConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending run job request...")
            val response = client.post("/runJob")
                .putHeader("content-type", "application/json")
                .rxSendJson(runJobConfig)
                .toBlocking()
                .value()
            logger.info("Request completed with response: ${response.bodyAsString()}")
        } catch (e: Exception) {
            logger.error("An error occurred while sending a request", e)
            throw RuntimeException(e)
        } finally {
            client.close()
        }
    }
}

