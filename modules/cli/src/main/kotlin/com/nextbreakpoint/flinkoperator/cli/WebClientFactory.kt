package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(connectionConfig: ConnectionConfig): WebClient
}
