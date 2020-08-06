package com.nextbreakpoint.flink.k8s.controller.core

data class Result<T>(val status: ResultStatus, val output: T) {
    fun isSuccessful() = status == ResultStatus.OK
}
