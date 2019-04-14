package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobCancelConfig
import org.apache.log4j.Logger

class CancelJob {
    companion object {
        val logger = Logger.getLogger(CancelJob::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, cancelConfig: JobCancelConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending cancel job request...")
            val response = client.post("/cancelJob")
                .putHeader("content-type", "application/json")
                .rxSendJson(cancelConfig)
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

