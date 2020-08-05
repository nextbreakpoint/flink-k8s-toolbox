package com.nextbreakpoint.flinkoperator.client.factory

import com.nextbreakpoint.flinkoperator.client.command.LaunchBootstrap
import com.nextbreakpoint.flinkoperator.client.command.ClusterCreate
import com.nextbreakpoint.flinkoperator.client.command.ClusterDelete
import com.nextbreakpoint.flinkoperator.client.command.ClusterScale
import com.nextbreakpoint.flinkoperator.client.command.ClusterStart
import com.nextbreakpoint.flinkoperator.client.command.ClusterStatus
import com.nextbreakpoint.flinkoperator.client.command.ClusterStop
import com.nextbreakpoint.flinkoperator.client.command.ClustersList
import com.nextbreakpoint.flinkoperator.client.command.JobDetails
import com.nextbreakpoint.flinkoperator.client.command.JobManagerMetrics
import com.nextbreakpoint.flinkoperator.client.command.JobMetrics
import com.nextbreakpoint.flinkoperator.client.command.LaunchOperator
import com.nextbreakpoint.flinkoperator.client.command.SavepointForget
import com.nextbreakpoint.flinkoperator.client.command.SavepointTrigger
import com.nextbreakpoint.flinkoperator.client.command.LaunchSupervisor
import com.nextbreakpoint.flinkoperator.client.command.TaskManagerDetails
import com.nextbreakpoint.flinkoperator.client.command.TaskManagerMetrics
import com.nextbreakpoint.flinkoperator.client.command.TaskManagersList

object CommandDefaultFactory : CommandFactory {
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