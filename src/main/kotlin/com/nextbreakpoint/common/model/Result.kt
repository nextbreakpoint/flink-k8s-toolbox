package com.nextbreakpoint.common.model

data class Result<T>(val status: ResultStatus, val output: T)
