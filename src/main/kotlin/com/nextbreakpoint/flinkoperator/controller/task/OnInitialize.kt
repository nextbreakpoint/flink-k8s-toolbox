package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class OnInitialize : Task() {
    override fun execute(context: TaskContext) {
        context.onResourceInitialise()
    }
}