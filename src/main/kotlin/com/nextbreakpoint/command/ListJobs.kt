package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobListConfig
import org.apache.log4j.Logger

class ListJobs {
    companion object {
        val logger = Logger.getLogger(ListJobs::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, listConfig: JobListConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending list jobs request...")
            val response = client.post("/listJobs")
                .putHeader("content-type", "application/json")
                .rxSendJson(listConfig)
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

