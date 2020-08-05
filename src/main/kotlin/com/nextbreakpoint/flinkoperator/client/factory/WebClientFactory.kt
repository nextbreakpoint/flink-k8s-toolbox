package com.nextbreakpoint.flinkoperator.client.factory

import com.nextbreakpoint.flinkoperator.common.ConnectionConfig
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(connectionConfig: ConnectionConfig): WebClient
}
