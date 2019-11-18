package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster

object Configuration {
    fun getSavepointMode(flinkCluster: V1FlinkCluster) : String =
        flinkCluster.spec?.operator?.savepointMode ?: "MANUAL"

    fun getSavepointPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.spec?.operator?.savepointPath?.trim('\"')

    fun getSavepointInterval(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.spec?.operator?.savepointInterval?.toLong() ?: 36000

    fun getSavepointTargetPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.spec?.operator?.savepointTargetPath?.trim()

    fun getJobRestartPolicy(flinkCluster: V1FlinkCluster) : String =
        flinkCluster.spec?.operator?.jobRestartPolicy ?: "NEVER"
}