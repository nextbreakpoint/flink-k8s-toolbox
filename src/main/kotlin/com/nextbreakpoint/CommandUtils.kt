package com.nextbreakpoint

import com.google.common.io.ByteStreams.copy
import com.nextbreakpoint.flinkclient.api.DefaultApi
import io.kubernetes.client.ApiClient
import io.kubernetes.client.PortForward
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.util.Config
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.client.WebClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import java.io.File
import java.io.FileInputStream
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

object CommandUtils {
    fun flinkApi(host: String = "localhost", port: Int = 8081): DefaultApi {
        val flinkApi = DefaultApi()
        flinkApi.apiClient.basePath = "http://$host:$port"
        flinkApi.apiClient.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
        flinkApi.apiClient.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
        flinkApi.apiClient.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
//        flinkApi.apiClient.isDebugging = true
        return flinkApi
    }

    fun createKubernetesClient(kubeConfig: String?): ApiClient? {
        val client = if (kubeConfig?.isNotBlank() == true) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
        client.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
        client.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
        client.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
//            client.isDebugging = true
        return client
    }

    @ExperimentalCoroutinesApi
    fun forwardPort(
        pod: V1Pod?,
        localPort: Int,
        port: Int,
        stop: Channel<Int>
    ): Thread {
        return Thread(
            Runnable {
                var stdout : Thread? = null
                var stdin : Thread? = null
                try {
                    val forwardResult = PortForward().forward(pod, listOf(port))
                    val serverSocket = ServerSocket(localPort)
                    val clientSocket = serverSocket.accept()
                    stop.invokeOnClose {
                        try {
                            clientSocket.close()
                        } catch (e: Exception) {
                        }
                        try {
                            serverSocket.close()
                        } catch (e: Exception) {
                        }
                    }
                    stdout = Thread(
                        Runnable {
                            try {
                                copy(clientSocket.inputStream, forwardResult.getOutboundStream(port))
                            } catch (ex: Exception) {
                            }
                        })
                    stdin = Thread(
                        Runnable {
                            try {
                                copy(forwardResult.getInputStream(port), clientSocket.outputStream)
                            } catch (ex: Exception) {
                            }
                        })
                    stdout.start()
                    stdin.start()
                    stdout.join()
                    stdin.interrupt()
                    stdin.join()
                    stdout = null
                    stdin = null
                } catch (e: Exception) {
                    e.printStackTrace()
                } finally {
                    stdout?.interrupt()
                    stdin?.interrupt()
                    stdout?.join()
                    stdin?.join()
                }
            })
    }

    @Throws(InterruptedException::class)
    fun processExec(proc: Process) {
        var stdout : Thread? = null
        var stderr : Thread? = null
        try {
            stdout = Thread(
                Runnable {
                    try {
                        copy(proc.inputStream, System.out)
                    } catch (ex: Exception) {
                    }
                })
            stderr = Thread(
                Runnable {
                    try {
                        copy(proc.errorStream, System.out)
                    } catch (ex: Exception) {
                    }
                })
            stdout.start()
            stderr.start()
            proc.waitFor(60, TimeUnit.SECONDS)
            stdout.interrupt()
            stderr.interrupt()
            stdout.join()
            stderr.join()
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            stdout?.interrupt()
            stderr?.interrupt()
            stdout?.join()
            stderr?.join()
        }
    }

    fun createWebClient(host: String = "localhost", port: Int): WebClient {
        val clientOptions = WebClientOptions()
//        clientOptions.logActivity = true
        clientOptions.isFollowRedirects = true
        clientOptions.defaultHost = host
        clientOptions.defaultPort = port
        return WebClient.create(Vertx.vertx(), clientOptions)
    }
}