package com.nextbreakpoint.flink.cli.factory

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.core.JobCommand
import com.nextbreakpoint.flink.cli.core.LaunchCommand
import com.nextbreakpoint.flink.cli.core.OperatorCommand
import com.nextbreakpoint.flink.common.BootstrapOptions
import com.nextbreakpoint.flink.common.OperatorOptions
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.SupervisorOptions
import com.nextbreakpoint.flink.common.TaskManagerId

interface CommandFactory {
    fun createLaunchOperatorCommand() : LaunchCommand<OperatorOptions>

    fun createLaunchBootstrapCommand() : LaunchCommand<BootstrapOptions>

    fun createLaunchSupervisorCommand() : LaunchCommand<SupervisorOptions>

    fun createListClustersCommand() : OperatorCommand

    fun createCreateClusterCommand() : ClusterCommand<String>

    fun createDeleteClusterCommand() : ClusterCommand<Void?>

    fun createStartClusterCommand() : ClusterCommand<StartOptions>

    fun createStopClusterCommand() : ClusterCommand<StopOptions>

    fun createScaleClusterCommand() : ClusterCommand<ScaleClusterOptions>

    fun createGetClusterStatusCommand(): ClusterCommand<Void?>

    fun createGetJobManagerMetricsCommand() : ClusterCommand<Void?>

    fun createListTaskManagersCommand() : ClusterCommand<Void?>

    fun createGetTaskManagerDetailsCommand() : ClusterCommand<TaskManagerId>

    fun createGetTaskManagerMetricsCommand() : ClusterCommand<TaskManagerId>

    fun createListJobsCommand() : ClusterCommand<Void?>

    fun createStartJobCommand() : JobCommand<StartOptions>

    fun createStopJobCommand() : JobCommand<StopOptions>

    fun createScaleJobCommand() : JobCommand<ScaleJobOptions>

    fun createGetJobStatusCommand(): JobCommand<Void?>

    fun createTriggerSavepointCommand(): JobCommand<Void?>

    fun createForgetSavepointCommand(): JobCommand<Void?>

    fun createGetJobDetailsCommand() : JobCommand<Void?>

    fun createGetJobMetricsCommand() : JobCommand<Void?>
}