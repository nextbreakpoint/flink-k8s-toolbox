package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Address

abstract class Command<T>(val factory: WebClientFactory) {
    abstract fun run(address: Address, clusterName: String, args: T)
}

