package com.nextbreakpoint.flinkoperator.utility

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.GsonBuilder
import java.io.File
import java.net.URL
import java.nio.file.Files
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper

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

            val expandedDefinitions = expandDefinitions(outputDefinitions)

            File("definitions").mkdirs()

            expandedDefinitions.forEach {
                val json = GsonBuilder().setPrettyPrinting().create().toJson(it.value)
                println(json)
                val jsonNodeTree = ObjectMapper().readTree(json)
                val jsonAsYaml = YAMLMapper().writeValueAsString(jsonNodeTree)
                Files.write(File("definitions", it.key + ".yaml").toPath(), jsonAsYaml.toByteArray())
            }
        }

        private fun expandDefinitions(definitions: Map<String, Map<String, Any>?>): Map<String, Map<String, Any>?> {
            return definitions.mapValues {
                expandDefinition(it.value.orEmpty() as Map<String, Map<String, Any>>) as Map<String, Any>
            }.toMap()
        }

        private fun expandDefinition(definition: Map<String, Any>): Map<String, Map<String, Any>?> {
            val localDefinition = definition.toMutableMap() as MutableMap<String, Map<String, Any>?>
            val properties = localDefinition["properties"] as Map<String, Map<String, Any>>?
            val expandedProperties = properties.orEmpty().mapValues {
                if (it.value["type"] == "array") {
                    val items = it.value["items"] as Map<String, Map<String, Any>>?
                    val ref = items.orEmpty()["\$ref"] as String?
                    if (ref != null) {
                        val refType = ref.substring(ref.lastIndexOf('/') + 1)
                        val refDefinition = typeDefinitions[refType].orEmpty()
                        expandDefinition(refDefinition)
                    } else {
                        it.value as Map<String, Map<String, Any>>
                    }
                } else if (it.value["type"] == "object") {
                    val ref = it.value["\$ref"] as String?
                    if (ref != null) {
                        val refType = ref.substring(ref.lastIndexOf('/') + 1)
                        val refDefinition = typeDefinitions[refType].orEmpty()
                        expandDefinition(refDefinition)
                    } else {
                        expandDefinition(it.value)
                    }
                } else {
                    val ref = it.value["\$ref"] as String?
                    if (ref != null) {
                        val refType = ref.substring(ref.lastIndexOf('/') + 1)
                        val refDefinition = typeDefinitions[refType].orEmpty()
                        expandDefinition(refDefinition)
                    } else {
                        it.value as Map<String, Map<String, Any>>
                    }
                }
            }.toMap()
            localDefinition["properties"] = expandedProperties
            val additionalProperties = localDefinition["additionalProperties"] as Map<String, String>?
            if (additionalProperties != null) {
                val ref = additionalProperties["\$ref"]
                if (ref != null) {
                    val refType = ref.substring(ref.lastIndexOf('/') + 1)
                    val refDefinition = typeDefinitions[refType].orEmpty()
                    localDefinition["additionalProperties"] = expandDefinition(refDefinition) as Map<String, Any>
                }
            }
            return localDefinition.toMap()
        }

        private fun createOutputTypes(selectedTypes: List<String>): MutableSet<String> {
            val outputTypes = mutableSetOf<String>()
            typeDefinitions.filter { it.key in selectedTypes || ("V1" + it.key.substringAfterLast(".v1.")) in selectedTypes }.forEach {
                outputTypes.add(it.key)
            }
            return outputTypes
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
                val properties = definition.get("properties") as Map<String, Map<String, Any>>?
                properties?.forEach {
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