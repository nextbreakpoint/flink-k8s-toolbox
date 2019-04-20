package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.GetTaskManagerConfig
import org.apache.log4j.Logger

class GetTaskManagerMetrics {
    companion object {
        val logger = Logger.getLogger(GetTaskManagerMetrics::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, taskManagerConfig: GetTaskManagerConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending get taskmanager metrics request...")
            val response = client.post("/getTaskManagerMetrics")
                .putHeader("content-type", "application/json")
                .rxSendJson(taskManagerConfig)
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

