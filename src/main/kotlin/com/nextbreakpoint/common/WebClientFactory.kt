package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Address
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(params: Address): WebClient
}
