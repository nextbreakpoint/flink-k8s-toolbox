package com.nextbreakpoint.flinkoperator.server.vertex.core

import com.nextbreakpoint.flinkoperator.common.ClusterSelector

data class Command(val clusterSelector: ClusterSelector, val json: String)