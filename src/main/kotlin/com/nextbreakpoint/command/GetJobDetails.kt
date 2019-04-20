package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobDetailsConfig
import org.apache.log4j.Logger

class GetJobDetails {
    companion object {
        val logger = Logger.getLogger(GetJobDetails::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, jobDetailsConfig: JobDetailsConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending job details request...")
            val response = client.post("/getJobDetails")
                .putHeader("content-type", "application/json")
                .rxSendJson(jobDetailsConfig)
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

