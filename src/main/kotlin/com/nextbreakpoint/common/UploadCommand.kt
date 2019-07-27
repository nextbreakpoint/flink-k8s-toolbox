package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.FlinkOptions

interface UploadCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, clusterName: String, params: T)
}

