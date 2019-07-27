package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Address

abstract class CommandNoArgs(val factory: WebClientFactory) {
    abstract fun run(address: Address, clusterName: String)
}



