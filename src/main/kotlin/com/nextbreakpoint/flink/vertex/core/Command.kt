package com.nextbreakpoint.flink.vertex.core

import com.nextbreakpoint.flink.common.ResourceSelector

data class Command(val resourceSelector: ResourceSelector, val json: String)