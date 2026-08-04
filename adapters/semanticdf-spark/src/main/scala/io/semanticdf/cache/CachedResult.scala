package io.semanticdf.cache

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/** The materialised result of one cached query.
  *
  * Cache values are stored as `Array[Row]` (Spark's row representation
  * of the result) plus the schema. Both are required to rebuild a
  * `DataFrame` on a cache hit without re-executing the Spark plan.
  *
  * Why rows + schema and not the `DataFrame` itself: a `DataFrame`
  * is a lazy operator tree, not a value — to put one in a cache we
  * have to materialise it. The materialised form (rows + schema) is
  * what every `DataFrame.collect()` returns under the hood, so this
  * is the smallest representation that can rebuild a DataFrame.
  *
  * Memory: the row array is the dominant cost. A 100-row query
  * with 10 columns is a few KB. A 1M-row query is hundreds of MB.
  * The cache is bounded by `maxEntries` and evicts the LRU entry
  * on overflow — memory is bounded by `maxEntries × typical_row_size`. */
final case class CachedResult(
    rows:   Array[Row],
    schema: StructType,
)
