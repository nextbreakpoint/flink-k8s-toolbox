package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobSubmitConfig
import org.apache.log4j.Logger

class SubmitJob {
    companion object {
        val logger = Logger.getLogger(SubmitJob::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, submitConfig: JobSubmitConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending submit job request...")
            val response = client.post("/submitJob")
                .putHeader("content-type", "application/json")
                .rxSendJson(submitConfig)
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

