package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig
import io.vertx.rxjava.core.buffer.Buffer

object HttpUtils {
    fun postJson(factory: WebClientFactory, connectionConfig: ConnectionConfig, path: String, body: String) {
        try {
            val client = factory.create(connectionConfig)
            try {
                client.post(path)
                    .putHeader("content-type", "application/json")
                    .putHeader("accept", "application/json")
                    .rxSendBuffer(Buffer.buffer(body))
                    .doOnSuccess {
                        println(it.bodyAsString())
                    }
                    .toCompletable()
                    .await()
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    fun <T> putJson(factory: WebClientFactory, connectionConfig: ConnectionConfig, path: String, body: T) {
        try {
            val client = factory.create(connectionConfig)
            try {
                client.put(path)
                    .putHeader("content-type", "application/json")
                    .putHeader("accept", "application/json")
                    .rxSendJson(body)
                    .doOnSuccess {
                        println(it.bodyAsString())
                    }
                    .toCompletable()
                    .await()
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    fun delete(factory: WebClientFactory, connectionConfig: ConnectionConfig, path: String) {
        try {
            val client = factory.create(connectionConfig)
            try {
                client.delete(path)
                    .putHeader("accept", "application/json")
                    .rxSend()
                    .doOnSuccess {
                        println(it.bodyAsString())
                    }
                    .toCompletable()
                    .await()
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    fun get(factory: WebClientFactory, connectionConfig: ConnectionConfig, path: String) {
        try {
            val client = factory.create(connectionConfig)
            try {
                client.get(path)
                    .putHeader("accept", "application/json")
                    .rxSend()
                    .doOnSuccess {
                        println(it.bodyAsString())
                    }
                    .toCompletable()
                    .await()
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    fun get(factory: WebClientFactory, connectionConfig: ConnectionConfig, path: String, queryParams: List<Pair<String, String>>) {
        try {
            val client = factory.create(connectionConfig)
            try {
                val request = client.get(path)
                    .putHeader("accept", "application/json")

                queryParams.forEach {
                    request.addQueryParam(it.first, it.second)
                }

                request.rxSend()
                    .doOnSuccess {
                        println(it.bodyAsString())
                    }
                    .toCompletable()
                    .await()
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

