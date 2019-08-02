package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Address
import io.vertx.rxjava.core.buffer.Buffer

object Commands {
    fun postText(factory: WebClientFactory, address: Address, path: String, body: String) {
        try {
            val client = factory.create(address)
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

    fun <T> putJson(factory: WebClientFactory, address: Address, path: String, body: T) {
        try {
            val client = factory.create(address)
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

    fun delete(factory: WebClientFactory, address: Address, path: String) {
        try {
            val client = factory.create(address)
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

    fun get(factory: WebClientFactory, address: Address, path: String) {
        try {
            val client = factory.create(address)
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

    fun get(factory: WebClientFactory, address: Address, path: String, queryParams: List<Pair<String, String>>) {
        try {
            val client = factory.create(address)
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

