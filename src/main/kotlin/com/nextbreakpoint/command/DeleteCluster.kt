package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.ClusterDescriptor
import org.apache.log4j.Logger

class DeleteCluster {
    companion object {
        val logger = Logger.getLogger(DeleteCluster::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, descriptor: ClusterDescriptor) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending delete cluster request...")
            val response = client.post("/deleteCluster")
                .putHeader("content-type", "application/json")
                .rxSendJson(descriptor)
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

