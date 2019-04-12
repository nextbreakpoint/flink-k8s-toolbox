package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.ClusterConfig
import org.apache.log4j.Logger

class CreateCluster {
    companion object {
        val logger = Logger.getLogger(CreateCluster::class.simpleName)
    }

    fun run(apiConfig: ApiConfig, clusterConfig: ClusterConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            logger.info("Sending create cluster request...")
            val response = client.post("/createCluster")
                .putHeader("content-type", "application/json")
                .rxSendJson(clusterConfig)
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

