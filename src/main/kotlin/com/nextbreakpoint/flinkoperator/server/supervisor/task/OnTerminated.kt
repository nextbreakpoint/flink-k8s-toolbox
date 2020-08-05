package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext

class OnTerminated : Task() {
    override fun execute(context: TaskContext) {
        context.terminateCluster()
    }
}