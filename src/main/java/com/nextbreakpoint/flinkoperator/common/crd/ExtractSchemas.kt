package com.nextbreakpoint.flinkoperator.common.crd

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.net.URL

class ExtractSchemas {
    companion object {
        private val typeDefinitions = mutableMapOf<String, Map<String, Any>>()

        private val URL = "https://raw.githubusercontent.com/kubernetes/kubernetes/master/api/openapi-spec/swagger.json"

        @JvmStatic
        fun main(args: Array<String>) {
            val specification = ObjectMapper().readValue(URL(URL).openStream(), Map::class.java) as Map<String, Any>
            val definitions = specification.get("definitions") as Map<String, Map<String, Any>>
            processDefinitions(definitions)
//            typeDefinitions.forEach {
//                printDefinition(it.key, it.value)
//            }
            val selectedTypes = args.toList()
            val outputTypes = createOutputTypes(selectedTypes)
//            outputTypes.forEach {
//                printDefinition(it, typeDefinitions[it] ?: mapOf())
//            }
            val outputDefinitions = outputTypes.map { it to typeDefinitions[it] }.toMap()
            val newDefinitions = mapOf("definitions" to outputDefinitions)
            println(GsonBuilder().setPrettyPrinting().create().toJson(newDefinitions))
        }

        private fun createOutputTypes(selectedTypes: List<String>): MutableSet<String> {
            val outputTypes = mutableSetOf<String>()
            typeDefinitions.filter { it.key in selectedTypes || ("V1" + it.key.substringAfterLast(".v1.")) in selectedTypes }.forEach {
                outputTypes.add(it.key)
                selectProperties(it.value, outputTypes)
            }
            return outputTypes
        }

        private fun selectProperties(definition: Map<String, Any>, outputTypes: MutableSet<String>) {
            val props = definition.get("properties") as Map<String, Map<String, Any>>?
            props?.forEach {
                if (it.value.get("\$ref") != null) {
                    val ref = it.value.get("\$ref") as String
                    val type = ref.substring(ref.lastIndexOf('/') + 1)
                    if (!outputTypes.contains(type)) {
                        outputTypes.add(type)
                        selectProperties(typeDefinitions[type] ?: mapOf(), outputTypes)
                    }
                }
            }
        }

        private fun processDefinitions(definitions: Map<String, Map<String, Any>>) {
            definitions.forEach {
//                println("Add type definition ${it.key}")
                typeDefinitions.putIfAbsent(it.key, it.value)
            }
        }

        private fun printDefinition(key: String, definition: Map<String, Any>) {
            println("Type definition $key:")
            println("\ttype = ${definition.get("type")}")
            println("\tformat = ${definition.get("format")}")
            if (definition.get("type") == "object") {
                println("\tproperties = [")
                val props = definition.get("properties") as Map<String, Map<String, Any>>?
                props?.forEach {
                    if (it.value.get("\$ref") != null) {
                        val ref = it.value.get("\$ref") as String
                        val type = ref.substring(ref.lastIndexOf('/') + 1)
                        println("\t\t${it.key} = $type")
                    } else {
                        val type = it.value.get("type")
                        println("\t\t${it.key} = $type")
                    }
                }
                println("\t]")
            }
        }
    }
}