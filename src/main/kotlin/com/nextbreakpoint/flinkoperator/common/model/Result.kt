package com.nextbreakpoint.flinkoperator.common.model

data class Result<T>(val status: ResultStatus, val output: T)
