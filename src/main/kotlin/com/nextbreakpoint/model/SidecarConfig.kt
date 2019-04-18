package com.nextbreakpoint.model

val ARGUMENTS_PATTERN = "(--([^ ]+)=(\"[^=]+\"))|(--([^ ]+)=([^\"= ]+))|([^ ]+)"

data class SidecarConfig(
    val image: String,
    val pullSecrets: String,
    val pullPolicy: String?,
    val arguments: String?
)
