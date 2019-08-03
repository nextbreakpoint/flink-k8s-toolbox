package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ConnectionConfig
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(params: ConnectionConfig): WebClient
}
