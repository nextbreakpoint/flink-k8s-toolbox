package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.GetTaskManagerConfig
import org.apache.log4j.Logger

class GetTaskManagerDetails {
    companion object {
        val logger = Logger.getLogger(GetTaskManagerDetails::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, taskManagerConfig: GetTaskManagerConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending get taskmanager details request...")
            val response = client.post("/getTaskManagerDetails")
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

