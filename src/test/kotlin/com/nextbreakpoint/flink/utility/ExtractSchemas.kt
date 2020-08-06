package com.nextbreakpoint.flink.utility

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.google.gson.GsonBuilder
import java.io.File
import java.net.URL
import java.nio.file.Files

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

            val outputDefinitions = outputTypes.map { it to typeDefinitions[it].orEmpty() }.toMap()

            val expandedDefinitions = expandDefinitions(outputDefinitions)

            File("definitions").mkdirs()

            expandedDefinitions.forEach {
                saveDefinition(it.key, it.value)
            }
        }

        private fun saveDefinition(type: String, definition: Map<String, Any>) {
            val json = GsonBuilder().setPrettyPrinting().create().toJson(definition)
            println(json)
            val jsonNodeTree = ObjectMapper().readTree(json)
            val jsonAsYaml = YAMLMapper().writeValueAsString(jsonNodeTree)
            Files.write(File("definitions", type + ".yaml").toPath(), jsonAsYaml.toByteArray())
        }

        private fun expandDefinitions(definitions: Map<String, Map<String, Any>>): Map<String, Map<String, Any>> {
            return definitions.mapValues {
                expandDefinition(it.value as Map<String, Map<String, Any>>) as Map<String, Any>
            }.toMap()
        }

        private fun expandDefinition(definition: Map<String, Any>): Map<String, Map<String, Any>> {
            val localDefinition = definition.toMutableMap() as MutableMap<String, Map<String, Any>>
            val properties = localDefinition["properties"] as Map<String, Map<String, Any>>?
            val expandedProperties = properties.orEmpty().mapValues {
                val props = it.value.toMutableMap()
                if (props["type"] == "array") {
                    val items = props["items"] as Map<String, Map<String, Any>>?
                    if (items != null) {
                        val ref = items["\$ref"] as String?
                        if (ref != null) {
                            val refType = ref.substring(ref.lastIndexOf('/') + 1)
                            val refDefinition = typeDefinitions[refType].orEmpty()
                            val updatedItems = items.toMutableMap()
                            updatedItems.remove("\$ref")
                            updatedItems.putAll(expandDefinition(refDefinition))
                            val updatedProps = props.toMutableMap()
                            updatedProps.put("items", updatedItems)
                            updatedProps
                        } else {
                            props as Map<String, Map<String, Any>>
                        }
                    } else {
                        props as Map<String, Map<String, Any>>
                    }
                } else if (props["type"] == "object") {
                    val ref = props["\$ref"] as String?
                    if (ref != null) {
                        val refType = ref.substring(ref.lastIndexOf('/') + 1)
                        val refDefinition = typeDefinitions[refType].orEmpty()
                        val updatedProps = props.toMutableMap()
                        updatedProps.remove("\$ref")
                        updatedProps.putAll(expandDefinition(refDefinition))
                        updatedProps
                    } else {
                        expandDefinition(props)
                    }
                } else {
                    val ref = props["\$ref"] as String?
                    if (ref != null) {
                        val refType = ref.substring(ref.lastIndexOf('/') + 1)
                        val refDefinition = typeDefinitions[refType].orEmpty()
                        val updatedProps = props.toMutableMap()
                        updatedProps.remove("\$ref")
                        updatedProps.putAll(expandDefinition(refDefinition))
                        updatedProps
                    } else {
                        props as Map<String, Map<String, Any>>
                    }
                }
            }.toMap()
            localDefinition["properties"] = expandedProperties
            val additionalProperties = localDefinition["additionalProperties"]
            if (additionalProperties != null) {
                val ref = additionalProperties["\$ref"] as String?
                if (ref != null) {
                    val refType = ref.substring(ref.lastIndexOf('/') + 1)
                    val refDefinition = typeDefinitions[refType].orEmpty()
                    val updatedProps = additionalProperties.toMutableMap()
                    updatedProps.remove("\$ref")
                    updatedProps.putAll(expandDefinition(refDefinition) as Map<String, Any>)
                    localDefinition["additionalProperties"] = updatedProps
                }
            }
            return localDefinition.toMap()
        }

        private fun createOutputTypes(selectedTypes: List<String>): Set<String> {
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