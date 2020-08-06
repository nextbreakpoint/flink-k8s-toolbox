package com.nextbreakpoint.flink.cli.factory

import com.nextbreakpoint.flink.common.ConnectionConfig
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(connectionConfig: ConnectionConfig): WebClient
}
