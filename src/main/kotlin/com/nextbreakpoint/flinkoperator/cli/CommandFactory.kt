package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.BootstrapOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorOptions
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.SupervisorOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId

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