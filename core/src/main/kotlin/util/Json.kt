package com.amplitude.util

import com.charleskorn.kaml.Yaml
import com.charleskorn.kaml.YamlConfiguration
import kotlinx.serialization.json.Json

val json = Json {
    ignoreUnknownKeys = true
}

val yaml = Yaml(configuration = YamlConfiguration(strictMode = false))
