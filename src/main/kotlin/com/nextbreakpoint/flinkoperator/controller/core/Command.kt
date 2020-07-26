package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector

data class Command(val clusterSelector: ClusterSelector, val json: String)