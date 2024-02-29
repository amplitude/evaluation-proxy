fun user(
    userId: String? = null,
    deviceId: String? = null,
    userProperties: Map<String, Any?>? = null,
    groups: Map<String, Set<String>>? = null,
    groupProperties: Map<String, Map<String, Map<String, Any?>>>? = null
): MutableMap<String, Any?> {
    return mutableMapOf(
        "user_id" to userId,
        "device_id" to deviceId,
        "user_properties" to userProperties,
        "groups" to groups,
        "group_properties" to groupProperties
    )
}
