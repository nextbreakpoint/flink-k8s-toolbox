package com.nextbreakpoint.flink.vertex.core

import com.nextbreakpoint.flink.common.JobSelector

data class JobCommand(val selector: JobSelector, val json: String)