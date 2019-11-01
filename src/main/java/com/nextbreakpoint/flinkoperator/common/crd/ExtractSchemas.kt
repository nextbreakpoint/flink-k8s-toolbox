package com.nextbreakpoint.flinkoperator.common.crd

import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URL

class ExtractSchemas {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val specification = ObjectMapper().readValue(URL("https://raw.githubusercontent.com/kubernetes/kubernetes/master/api/openapi-spec/swagger.json").openStream(), Map::class.java) as Map<String, Any?>
            val definitions = specification.get("definitions") as Map<String, Map<String, Any?>>
            printObject(definitions)
        }

        private fun printObject(definitions: Map<String, Map<String, Any?>>) {
            definitions.forEach {
                println("${it.key}")
                val props = it.value.get("properties") as Map<String, Map<String, Any?>>?
                props?.forEach {
                    if (it.value.get("type") == null && it.value.get("\$ref") != null) {
                        val type = it.value.get("\$ref") as String
                        println("\t${it.key} = ${type.substring(type.lastIndexOf('/') + 1)}")
                    } else {
                        println("\t${it.key} = ${it.value.get("type")}")
                    }
                }
            }
        }
    }
}