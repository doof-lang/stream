# std/stream

Composable stream utilities. Provides `Chain<T>` — a fluent wrapper around any `Stream<T>` — and `blobStreamToLineStream`, which converts a stream of raw byte chunks into a stream of decoded UTF-8 lines.

## Usage

```doof
import { Chain } from "std/stream"

// Wrap any Stream<T> in a Chain to use the fluent API
numbers: Stream<int> = Counter { end: 20 }

result := Chain { source: numbers }
  .filter(=> it % 2 == 0)
  .map(=> it * it)
  .take(5)
  .collect()

// result == [4, 16, 36, 64, 100]
```

```doof
import { blobStreamToLineStream } from "std/stream"

// Convert a blob chunk stream into text lines
lineStream := blobStreamToLineStream(blobChunks)
for line of lineStream {
  println(line)
}
```

## Exports

### `Chain<T>`

A `Stream<T>` wrapper that exposes a fluent transformation API. Each operation returns a new `Chain` so calls can be chained without consuming the source immediately. The underlying stream is pulled lazily when iterating or calling `collect()`.

#### Construction

```doof
Chain { source: myStream }
```

#### `next(): T | null`

Pull the next item from the chain. Returns `null` when the stream is exhausted.

#### `filter(pred: (it: T): bool): Chain<T>`

Return a new chain that skips items for which `pred` returns `false`.

```doof
.filter(=> it > 0)
```

#### `map<U>(transform: (it: T): U): Chain<U>`

Return a new chain that applies `transform` to each item, changing the element type to `U`.

```doof
.map(=> string(it))
```

#### `take(count: int): Chain<T>`

Return a new chain that yields at most `count` items.

```doof
.take(10)
```

#### `collect(): T[]`

Consume the chain and return all remaining items as an array.

```doof
values := Chain { source: stream }.filter(=> it != "").collect()
```

---

### `blobStreamToLineStream(source: Stream<readonly byte[]>): Stream<string>`

Convert a stream of raw byte-array chunks into a stream of UTF-8 decoded lines. Handles `\n`, `\r`, and `\r\n` line endings. Lines that span chunk boundaries are assembled correctly. A trailing line without a terminator is emitted at the end of the stream.

This is the building block used by `fs.readLineStream`.

```doof
import { blobStreamToLineStream } from "std/stream"

lineStream := blobStreamToLineStream(myBlobChunkStream)
for line of lineStream {
  process(line)
}
```
