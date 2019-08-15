package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster

object OperatorParameters {
    fun getSavepointMode(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.spec?.flinkOperator?.savepointMode ?: "MANUAL"

    fun getSavepointPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.spec?.flinkOperator?.savepointPath?.trim('\"')

    fun getSavepointInterval(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.spec?.flinkOperator?.savepointInterval?.toLong() ?: 36000

    fun getSavepointTargetPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.spec?.flinkOperator?.savepointTargetPath?.trim()
}