package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.cli.command.LaunchBootstrap
import com.nextbreakpoint.flinkoperator.cli.command.ClusterCreate
import com.nextbreakpoint.flinkoperator.cli.command.ClusterDelete
import com.nextbreakpoint.flinkoperator.cli.command.ClusterScale
import com.nextbreakpoint.flinkoperator.cli.command.ClusterStart
import com.nextbreakpoint.flinkoperator.cli.command.ClusterStatus
import com.nextbreakpoint.flinkoperator.cli.command.ClusterStop
import com.nextbreakpoint.flinkoperator.cli.command.ClustersList
import com.nextbreakpoint.flinkoperator.cli.command.JobDetails
import com.nextbreakpoint.flinkoperator.cli.command.JobManagerMetrics
import com.nextbreakpoint.flinkoperator.cli.command.JobMetrics
import com.nextbreakpoint.flinkoperator.cli.command.LaunchOperator
import com.nextbreakpoint.flinkoperator.cli.command.SavepointForget
import com.nextbreakpoint.flinkoperator.cli.command.SavepointTrigger
import com.nextbreakpoint.flinkoperator.cli.command.LaunchSupervisor
import com.nextbreakpoint.flinkoperator.cli.command.TaskManagerDetails
import com.nextbreakpoint.flinkoperator.cli.command.TaskManagerMetrics
import com.nextbreakpoint.flinkoperator.cli.command.TaskManagersList

object DefaultCommandFactory : CommandFactory {
    override fun createLaunchOperatorCommand() = LaunchOperator()

    override fun createLaunchBootstrapCommand() = LaunchBootstrap()

    override fun createLaunchSupervisorCommand() = LaunchSupervisor()

    override fun createListClustersCommand() = ClustersList()

    override fun createCreateClusterCommand() = ClusterCreate()

    override fun createDeleteClusterCommand() = ClusterDelete()

    override fun createStartClusterCommand() = ClusterStart()

    override fun createStopClusterCommand() = ClusterStop()

    override fun createScaleClusterCommand() = ClusterScale()

    override fun createGetClusterStatusCommand() = ClusterStatus()

    override fun createTriggerSavepointCommand() = SavepointTrigger()

    override fun createForgetSavepointCommand() = SavepointForget()

    override fun createGetJobDetailsCommand() = JobDetails()

    override fun createGetJobMetricsCommand() = JobMetrics()

    override fun createGetJobManagerMetricsCommand() = JobManagerMetrics()

    override fun createListTaskManagersCommand() = TaskManagersList()

    override fun createGetTaskManagerDetailsCommand() = TaskManagerDetails()

    override fun createGetTaskManagerMetricsCommand() = TaskManagerMetrics()
}