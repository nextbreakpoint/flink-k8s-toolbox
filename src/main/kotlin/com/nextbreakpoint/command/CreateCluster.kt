package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.ClusterConfig

class CreateCluster {
    fun run(apiConfig: ApiConfig, clusterConfig: ClusterConfig) {
        try {
            val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
            val response = client.post("/createCluster")
                .putHeader("content-type", "application/json")
                .rxSendJson(clusterConfig)
                .toBlocking()
                .value()
                .bodyAsString()
            println(response)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

