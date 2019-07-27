package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Address

object Commands {
    fun <T> post(factory: WebClientFactory, address: Address, path: String, body: T?) {
        try {
            val client = factory.create(address)
            try {
                if (body != null) {
                    client.post(path)
                        .putHeader("content-type", "application/json")
                        .putHeader("accept", "application/json")
                        .rxSendJson(body)
                        .doOnSuccess {
                            println(it.bodyAsString())
                        }
                        .toCompletable()
                        .await()
                } else {
                    client.post(path)
                        .putHeader("content-type", "application/json")
                        .putHeader("accept", "application/json")
                        .rxSend()
                        .doOnSuccess {
                            println(it.bodyAsString())
                        }
                        .toCompletable()
                        .await()
                }
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    fun <T> put(factory: WebClientFactory, address: Address, path: String, body: T?) {
        try {
            val client = factory.create(address)
            try {
                if (body != null) {
                    client.put(path)
                        .putHeader("content-type", "application/json")
                        .putHeader("accept", "application/json")
                        .rxSendJson(body)
                        .doOnSuccess {
                            println(it.bodyAsString())
                        }
                        .toCompletable()
                        .await()
                } else {
                    client.put(path)
                        .putHeader("content-type", "application/json")
                        .putHeader("accept", "application/json")
                        .rxSend()
                        .doOnSuccess {
                            println(it.bodyAsString())
                        }
                        .toCompletable()
                        .await()
                }
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

