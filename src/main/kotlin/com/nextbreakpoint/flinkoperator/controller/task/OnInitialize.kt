package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import org.apache.log4j.Logger

class OnInitialize(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        context.initializeAnnotations()
        context.initializeStatus()
        context.updateDigests()
        context.addFinalizer()

        context.setClusterStatus(ClusterStatus.Starting)
    }
}