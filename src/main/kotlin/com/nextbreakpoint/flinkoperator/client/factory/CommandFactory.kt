package com.nextbreakpoint.flinkoperator.client.factory

import com.nextbreakpoint.flinkoperator.client.core.ClusterCommand
import com.nextbreakpoint.flinkoperator.client.core.ClusterCommandNoArgs
import com.nextbreakpoint.flinkoperator.client.core.LaunchCommand
import com.nextbreakpoint.flinkoperator.client.core.OperatorCommand
import com.nextbreakpoint.flinkoperator.common.BootstrapOptions
import com.nextbreakpoint.flinkoperator.common.OperatorOptions
import com.nextbreakpoint.flinkoperator.common.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.StartOptions
import com.nextbreakpoint.flinkoperator.common.StopOptions
import com.nextbreakpoint.flinkoperator.common.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.TaskManagerId

interface CommandFactory {
    fun createLaunchOperatorCommand() : LaunchCommand<OperatorOptions>

    fun createLaunchBootstrapCommand() : LaunchCommand<BootstrapOptions>

    fun createLaunchSupervisorCommand() : LaunchCommand<SupervisorOptions>

    fun createListClustersCommand() : OperatorCommand

    fun createCreateClusterCommand() : ClusterCommand<String>

    fun createDeleteClusterCommand() : ClusterCommandNoArgs

    fun createStartClusterCommand() : ClusterCommand<StartOptions>

    fun createStopClusterCommand() : ClusterCommand<StopOptions>

    fun createScaleClusterCommand() : ClusterCommand<ScaleOptions>

    fun createGetClusterStatusCommand(): ClusterCommandNoArgs

    fun createTriggerSavepointCommand(): ClusterCommandNoArgs

    fun createForgetSavepointCommand(): ClusterCommandNoArgs

    fun createGetJobDetailsCommand() : ClusterCommandNoArgs

    fun createGetJobMetricsCommand() : ClusterCommandNoArgs

    fun createGetJobManagerMetricsCommand() : ClusterCommandNoArgs

    fun createListTaskManagersCommand() : ClusterCommandNoArgs

    fun createGetTaskManagerDetailsCommand() : ClusterCommand<TaskManagerId>

    fun createGetTaskManagerMetricsCommand() : ClusterCommand<TaskManagerId>
}