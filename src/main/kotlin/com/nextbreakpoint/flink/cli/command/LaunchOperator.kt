package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.LaunchCommand
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.OperatorOptions
import com.nextbreakpoint.flink.vertex.OperatorLauncher

class LaunchOperator : LaunchCommand<OperatorOptions> {
    override fun run(flinkOptions: FlinkOptions, namespace: String, options: OperatorOptions) {
        val launcher = OperatorLauncher(flinkOptions, namespace, options)

        launcher.launch()

        launcher.waitUntilInterrupted()
    }
}
