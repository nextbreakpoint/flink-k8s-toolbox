package com.nextbreakpoint.common

import com.nextbreakpoint.command.ClusterCreate
import com.nextbreakpoint.command.ClusterDelete
import com.nextbreakpoint.command.ClusterStart
import com.nextbreakpoint.command.ClusterStop
import com.nextbreakpoint.command.JobDetails
import com.nextbreakpoint.command.JobManagerMetrics
import com.nextbreakpoint.command.JobMetrics
import com.nextbreakpoint.command.JobScale
import com.nextbreakpoint.command.LaunchOperator
import com.nextbreakpoint.command.TaskManagerDetails
import com.nextbreakpoint.command.TaskManagerMetrics
import com.nextbreakpoint.command.TaskManagersList
import com.nextbreakpoint.command.UploadJAR

object DefaultCommandFactory : CommandFactory {
    override fun createRunOperatorCommand() = LaunchOperator()

    override fun createUploadJARCommand() = UploadJAR()

    override fun createCreateClusterCommand() = ClusterCreate()

    override fun createDeleteClusterCommand() = ClusterDelete()

    override fun createStartClusterCommand() = ClusterStart()

    override fun createStopClusterCommand() = ClusterStop()

    override fun createScaleJobCommand() = JobScale()

    override fun createGetJobDetailsCommand() = JobDetails()

    override fun createGetJobMetricsCommand() = JobMetrics()

    override fun createGetJobManagerMetricsCommand() = JobManagerMetrics()

    override fun createListTaskManagersCommand() = TaskManagersList()

    override fun createGetTaskManagerDetailsCommand() = TaskManagerDetails()

    override fun createGetTaskManagerMetricsCommand() = TaskManagerMetrics()
}