package com.nextbreakpoint.flink.vertex.core

import com.nextbreakpoint.flink.common.ClusterSelector

data class ClusterCommand(val selector: ClusterSelector, val json: String)