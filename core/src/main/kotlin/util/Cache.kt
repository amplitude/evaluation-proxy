package com.amplitude.util

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.HashMap

/**
 * Least recently used (LRU) cache with TTL for cache entries.
 */
internal class Cache<K, V>(
    private val capacity: Int,
    private val ttlMillis: Long = 0,
) {
    private class Node<K, V>(
        var key: K? = null,
        var value: V? = null,
        var prev: Node<K, V>? = null,
        var next: Node<K, V>? = null,
        var ts: Long = System.currentTimeMillis(),
    )

    private var count = 0
    private val map: MutableMap<K, Node<K, V>?> = HashMap()
    private val head: Node<K, V> = Node()
    private val tail: Node<K, V> = Node()
    private val mutex = Mutex()
    private val timeout = ttlMillis > 0

    init {
        head.next = tail
        tail.prev = head
    }

    suspend fun get(key: K): V? =
        mutex.withLock {
            val n = map[key] ?: return null
            if (timeout && n.ts + ttlMillis < System.currentTimeMillis()) {
                removeNodeForKey(key)
                return null
            }
            updateInternal(n)
            return n.value
        }

    suspend fun set(
        key: K,
        value: V,
    ) = mutex.withLock {
        var n = map[key]
        if (n == null) {
            n = Node(key, value)
            map[key] = n
            addInternal(n)
            ++count
        } else {
            n.value = value
            n.ts = System.currentTimeMillis()
            updateInternal(n)
        }
        if (count > capacity) {
            val del = tail.prev?.key
            if (del != null) {
                removeNodeForKey(del)
            }
        }
    }

    suspend fun remove(key: K): Unit =
        mutex.withLock {
            removeNodeForKey(key)
        }

    private fun removeNodeForKey(key: K) {
        val n = map[key] ?: return
        removeInternal(n)
        map.remove(n.key)
        --count
    }

    private fun updateInternal(node: Node<K, V>) {
        removeInternal(node)
        addInternal(node)
    }

    private fun addInternal(node: Node<K, V>) {
        val after = head.next
        head.next = node
        node.prev = head
        node.next = after
        after!!.prev = node
    }

    private fun removeInternal(node: Node<K, V>) {
        val before = node.prev
        val after = node.next
        before!!.next = after
        after!!.prev = before
    }
}
