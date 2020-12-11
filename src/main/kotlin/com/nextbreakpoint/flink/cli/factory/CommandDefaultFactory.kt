package com.nextbreakpoint.flink.cli.factory

import com.nextbreakpoint.flink.cli.command.ClusterCreate
import com.nextbreakpoint.flink.cli.command.ClusterDelete
import com.nextbreakpoint.flink.cli.command.ClusterScale
import com.nextbreakpoint.flink.cli.command.ClusterStart
import com.nextbreakpoint.flink.cli.command.ClusterStatus
import com.nextbreakpoint.flink.cli.command.ClusterStop
import com.nextbreakpoint.flink.cli.command.ClusterUpdate
import com.nextbreakpoint.flink.cli.command.ClustersList
import com.nextbreakpoint.flink.cli.command.DeploymentStatus
import com.nextbreakpoint.flink.cli.command.DeploymentsList
import com.nextbreakpoint.flink.cli.command.JobCreate
import com.nextbreakpoint.flink.cli.command.JobDelete
import com.nextbreakpoint.flink.cli.command.JobDetails
import com.nextbreakpoint.flink.cli.command.JobManagerMetrics
import com.nextbreakpoint.flink.cli.command.JobMetrics
import com.nextbreakpoint.flink.cli.command.JobScale
import com.nextbreakpoint.flink.cli.command.JobStart
import com.nextbreakpoint.flink.cli.command.JobStatus
import com.nextbreakpoint.flink.cli.command.JobStop
import com.nextbreakpoint.flink.cli.command.JobUpdate
import com.nextbreakpoint.flink.cli.command.JobsList
import com.nextbreakpoint.flink.cli.command.LaunchBootstrap
import com.nextbreakpoint.flink.cli.command.LaunchOperator
import com.nextbreakpoint.flink.cli.command.LaunchSupervisor
import com.nextbreakpoint.flink.cli.command.SavepointForget
import com.nextbreakpoint.flink.cli.command.SavepointTrigger
import com.nextbreakpoint.flink.cli.command.TaskManagerDetails
import com.nextbreakpoint.flink.cli.command.TaskManagerMetrics
import com.nextbreakpoint.flink.cli.command.TaskManagersList

object CommandDefaultFactory : CommandFactory {
    override fun createLaunchOperatorCommand() = LaunchOperator()

    override fun createLaunchBootstrapCommand() = LaunchBootstrap()

    override fun createLaunchSupervisorCommand() = LaunchSupervisor()

    override fun createListDeploymentsCommand() = DeploymentsList()

    override fun createGetDeploymentStatusCommand() = DeploymentStatus()

    override fun createListClustersCommand() = ClustersList()

    override fun createCreateClusterCommand() = ClusterCreate()

    override fun createDeleteClusterCommand() = ClusterDelete()

    override fun createUpdateClusterCommand() = ClusterUpdate()

    override fun createStartClusterCommand() = ClusterStart()

    override fun createStopClusterCommand() = ClusterStop()

    override fun createScaleClusterCommand() = ClusterScale()

    override fun createGetJobManagerMetricsCommand() = JobManagerMetrics()

    override fun createListTaskManagersCommand() = TaskManagersList()

    override fun createGetTaskManagerDetailsCommand() = TaskManagerDetails()

    override fun createGetTaskManagerMetricsCommand() = TaskManagerMetrics()

    override fun createGetClusterStatusCommand() = ClusterStatus()

    override fun createListJobsCommand() = JobsList()

    override fun createCreateJobCommand() = JobCreate()

    override fun createDeleteJobCommand() = JobDelete()

    override fun createUpdateJobCommand() = JobUpdate()

    override fun createStartJobCommand() = JobStart()

    override fun createStopJobCommand() = JobStop()

    override fun createScaleJobCommand() = JobScale()

    override fun createGetJobStatusCommand() = JobStatus()

    override fun createTriggerSavepointCommand() = SavepointTrigger()

    override fun createForgetSavepointCommand() = SavepointForget()

    override fun createGetJobDetailsCommand() = JobDetails()

    override fun createGetJobMetricsCommand() = JobMetrics()
}