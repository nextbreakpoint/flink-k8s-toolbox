package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ScaleOptions
import com.nextbreakpoint.common.model.StartOptions
import com.nextbreakpoint.common.model.StopOptions
import com.nextbreakpoint.common.model.TaskManagerId
import com.nextbreakpoint.common.model.UploadOptions
import com.nextbreakpoint.model.V1FlinkClusterSpec
import com.nextbreakpoint.operator.OperatorConfig

interface CommandFactory {
    fun createRunOperatorCommand() : ServerCommand<OperatorConfig>

    fun createUploadJARCommand() : UploadCommand<UploadOptions>

    fun createCreateClusterCommand() : Command<V1FlinkClusterSpec>

    fun createDeleteClusterCommand() : CommandNoArgs

    fun createStartClusterCommand() : Command<StartOptions>

    fun createStopClusterCommand() : Command<StopOptions>

    fun createScaleJobCommand() : Command<ScaleOptions>

    fun createGetJobDetailsCommand() : CommandNoArgs

    fun createGetJobMetricsCommand() : CommandNoArgs

    fun createGetJobManagerMetricsCommand() : CommandNoArgs

    fun createListTaskManagersCommand() : CommandNoArgs

    fun createGetTaskManagerDetailsCommand() : Command<TaskManagerId>

    fun createGetTaskManagerMetricsCommand() : Command<TaskManagerId>
}