package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobMetricsConfig
import org.apache.log4j.Logger

class GetJobMetrics {
    companion object {
        val logger = Logger.getLogger(GetJobMetrics::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, jobMetricsConfig: JobMetricsConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending job metrics request...")
            val response = client.post("/jobMetrics")
                .putHeader("content-type", "application/json")
                .rxSendJson(jobMetricsConfig)
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

