package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.OperatorConfig
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.model.BootstrapOptions

interface CommandFactory {
    fun createRunOperatorCommand() : ServerCommand<OperatorConfig>

    fun createBootstrapCommand() : BootstrapCommand<BootstrapOptions>

    fun createCreateClusterCommand() : RemoteCommand<String>

    fun createDeleteClusterCommand() : RemoteCommandNoArgs

    fun createStartClusterCommand() : RemoteCommand<StartOptions>

    fun createStopClusterCommand() : RemoteCommand<StopOptions>

    fun createScaleClusterCommand() : RemoteCommand<ScaleOptions>

    fun createGetClusterStatusCommand(): RemoteCommandNoArgs

    fun createTriggerSavepointCommand(): RemoteCommandNoArgs

    fun createGetJobDetailsCommand() : RemoteCommandNoArgs

    fun createGetJobMetricsCommand() : RemoteCommandNoArgs

    fun createGetJobManagerMetricsCommand() : RemoteCommandNoArgs

    fun createListTaskManagersCommand() : RemoteCommandNoArgs

    fun createGetTaskManagerDetailsCommand() : RemoteCommand<TaskManagerId>

    fun createGetTaskManagerMetricsCommand() : RemoteCommand<TaskManagerId>
}