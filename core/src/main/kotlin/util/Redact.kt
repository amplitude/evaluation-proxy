package com.amplitude.util

import java.net.URI

/**
 * Redact a credential (deployment key, API key, etc.) for logging. Keeps only
 * the last 4 characters so a log line can still be correlated with a key
 * without disclosing it.
 */
fun String?.redacted(): String {
    if (this == null) return "null"
    if (this.length <= 4) return "****"
    return "****" + this.takeLast(4)
}

/**
 * Redact the user-info (username:password) portion of a URI for logging. For
 * example a Redis URI of the form `redis://user:secret@host:6379` is logged as
 * `redis://[redacted]@host:6379`. Falls back to a fully redacted value if the
 * URI cannot be parsed.
 */
fun String?.redactedUri(): String? {
    if (this == null) return null
    return try {
        val uri = URI(this)
        if (uri.rawUserInfo == null) {
            this
        } else {
            this.replaceFirst(uri.rawUserInfo + "@", "[redacted]@")
        }
    } catch (_: Exception) {
        "****"
    }
}
