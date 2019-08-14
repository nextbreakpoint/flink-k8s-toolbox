package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions

interface UploadCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, args: T)
}

