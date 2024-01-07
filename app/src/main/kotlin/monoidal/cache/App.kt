package monoidal.cache

import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*

val scope = CoroutineScope(Dispatchers.Default)

interface Cache<K, V> {
  suspend fun get(key: K): V?
  suspend fun set(key: K, value: V)
}

class BasicCache<K, V>(
    private val get: suspend (K) -> V?,
    private val set: suspend (K, V) -> Unit
) : Cache<K, V> {
  override suspend fun get(key: K): V? = this@BasicCache.get.invoke(key)
  override suspend fun set(key: K, value: V) = this@BasicCache.set.invoke(key, value)
}

fun <K, V, K2> Cache<K, V>.mapKeys(keyInv: (K2) -> K): BasicCache<K2, V> {
  val self = this
  return BasicCache(get = { k -> self.get(keyInv(k)) }, set = { k, v -> self.set(keyInv(k), v) })
}

fun <K, V, V2> Cache<K, V>.mapValues(f: (V) -> V2, fInv: (V2) -> V): Cache<K, V2> {
  val self = this
  return BasicCache(
      get = { k -> self.get(k)?.let { f(it) } },
      set = { k, v -> self.set(k, fInv(v)) }
  )
}

fun <K, V> Cache<K, V>.compose(other: Cache<K, V>): BasicCache<K, V> {
  val self = this
  return BasicCache(
      get = { k -> this.get(k) ?: other.get(k)?.also { self.set(k, it) } },
      set = { k, v ->
        val job1 = scope.async { self.set(k, v) }
        val job2 = scope.async { other.set(k, v) }
        awaitAll(job1, job2)
      }
  )
}

fun <K, V> Cache<K, V>.reuseInflight(dict: ConcurrentHashMap<K, Deferred<V?>>): BasicCache<K, V> {
  val self = this
  return BasicCache(
      get = { k ->
        val job =
            dict.getOrPut(k) {
              scope.async(
                  start = CoroutineStart.LAZY,
                  block = {
                    val v = self.get(k)
                    dict.remove(k)
                    return@async v
                  }
              )
            }

        job.await()
      },
      set = { k, v -> self.set(k, v) }
  )
}

fun interface Hashable {
  fun hash(): Int
}

class RamCache<K, V> : Cache<K, V> where K : Hashable {
  override suspend fun get(key: K): V {
    TODO("Not yet implemented")
  }

  override suspend fun set(key: K, value: V) {
    TODO("Not yet implemented")
  }
}

fun interface StringConvertible {
  fun string(): String
}

class DiskCache<K, V> : Cache<K, V> where K : StringConvertible {
  override suspend fun get(key: K): V {
    TODO("Not yet implemented")
  }

  override suspend fun set(key: K, value: V) {
    TODO("Not yet implemented")
  }
}

fun interface UrlConvertible {
  fun url(): URL
}

class NetworkCache<K, V> : Cache<K, V> where K : UrlConvertible {
  override suspend fun get(key: K): V {
    TODO("Not yet implemented")
  }

  override suspend fun set(key: K, value: V): Unit {
    TODO("Not yet implemented")
  }
}

suspend fun start() {
  val dict = ConcurrentHashMap<Hashable, Deferred<Int?>>()

  val ramCache = RamCache<Hashable, Int>()

  val diskCache =
      DiskCache<StringConvertible, Int>().mapKeys<StringConvertible, Int, Hashable> {
        StringConvertible { it.toString() }
      }

  val networkCache =
      NetworkCache<UrlConvertible, String>()
          .mapValues({ it.toInt() }, { it.toString() })
          .mapKeys<UrlConvertible, Int, Hashable> { UrlConvertible { URL(it.toString()) } }
          .reuseInflight(dict)

  val monoidalCache = ramCache.compose(diskCache).compose(networkCache)
  monoidalCache.set(Hashable { "Foo".hashCode() }, 999)
  val v = monoidalCache.get(Hashable { "Foo".hashCode() })

  println("Got value $v")
}

fun main() {
  runBlocking { start() }
}

