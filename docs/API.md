# std/stream Guide

`std/stream` contains composable helpers for Doof's pull-based `Stream<T>`
interface. Use `Chain<T>` to build lazy filter/map/take pipelines over an
existing stream, and `blobStreamToLineStream` to decode byte chunks into UTF-8
lines.

## Lazy Chains

`Chain` wraps another stream. Transformations are lazy: `filter`, `map`, and
`take` create another `Chain`, but the source is not consumed until the chain is
iterated or `collect()` is called.

```doof
result := Chain { source: numbers }
  .filter(=> it % 2 == 0)
  .map(=> it * it)
  .take(5)
  .collect()
```

`collect()` consumes the remaining items and returns a mutable array. If the
source stream has already been partially consumed, collection starts from the
current stream position.

## Line Decoding

`blobStreamToLineStream(source)` converts `Stream<readonly byte[]>` into
`Stream<string>`.

It handles:

- `\n`
- `\r`
- `\r\n`
- line breaks split across chunk boundaries
- a final line without a trailing line break

The helper decodes bytes as UTF-8 strings using the blob reader. It is the
building block used by `std/fs` line streaming.

## API

### `Chain<T>`

```doof
export class Chain<T> implements Stream<T>
```

Fields:

- `source: Stream<T>`

Methods:

- `next(): bool`
- `value(): T`
- `filter(pred: (it: T): bool): Chain<T>`
- `map<U>(transform: (it: T): U): Chain<U>`
- `take(count: int): Chain<T>`
- `collect(): T[]`

Defined in [index.do](../index.do).

### `blobStreamToLineStream`

```doof
export function blobStreamToLineStream(source: Stream<readonly byte[]>): Stream<string>
```

Convert byte chunks into decoded lines.

Defined in [index.do](../index.do).
