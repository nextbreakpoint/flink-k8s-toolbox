package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob

object FlinkJobConfiguration {
    fun getSavepointMode(flinkJob: V1FlinkJob) : String =
        flinkJob.spec?.savepoint?.savepointMode ?: "MANUAL"

    fun getSavepointPath(flinkJob: V1FlinkJob) : String? =
        flinkJob.spec?.savepoint?.savepointPath?.trim('\"')

    fun getSavepointInterval(flinkJob: V1FlinkJob) : Long =
        flinkJob.spec?.savepoint?.savepointInterval?.toLong() ?: 3600

    fun getSavepointTargetPath(flinkJob: V1FlinkJob) : String? =
        flinkJob.spec?.savepoint?.savepointTargetPath?.trim()

    fun getRestartPolicy(flinkJob: V1FlinkJob) : String =
        flinkJob.spec?.restart?.restartPolicy ?: "NEVER"

    fun getRestartDelay(flinkJob: V1FlinkJob) : Long =
        flinkJob.spec?.restart?.restartDelay?.toLong() ?: 60

    fun getRestartTimeout(flinkJob: V1FlinkJob) : Long =
        flinkJob.spec?.restart?.restartTimeout?.toLong() ?: 180
}