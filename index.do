import { BlobReader, BlobBuilder } from "std/blob"

class FilteredStream<T> implements Stream<T> {
  source: Stream<T>
  pred: (it: T): bool
  let currentValue: T | none = none

  next(): bool {
    while true {
      if !source.next() {
          return false
      }
      candidate := source.value()
      
      if (pred(candidate)) {
          currentValue = candidate
          return true
      }
    }
  }

  value(): T => currentValue!
}

class MappedStream<T, U> implements Stream<U> {
  source: Stream<T>
  transform: (it: T): U

  next(): bool =>source.next()
  value(): U => transform(source.value())
}

class TakeStream<T> implements Stream<T> {
  source: Stream<T>
  let remaining: int

  next(): bool {
    if remaining <= 0 {
      return false
    }
    if !source.next() {
      return false
    }
    remaining = remaining - 1
    return true
  }

  value(): T => source.value()
}

export class Chain<T> implements Stream<T> {
  source: Stream<T>

  next(): bool => source.next()
  value(): T => source.value()

  filter(pred: (it: T): bool): Chain<T> => Chain<T> { source: FilteredStream<T> { source, pred } }
  map<U>(transform: (it: T): U): Chain<U> => Chain<U> { source: MappedStream<T, U> { source, transform } }
  take(count: int): Chain<T> => Chain<T> { source: TakeStream<T> { source, remaining: count } }

  collect(): T[] {
    let values: T[] = []
    for item of source {
      values.push(item)
    }
    return values
  }
}

class DecodedLineStream implements Stream<string> {
  source: Stream<readonly byte[]>
  pendingLine: BlobBuilder = BlobBuilder()
  let current: BlobReader = BlobReader([])
  let currentValue: string | none = none
  lineBreakBytes: readonly byte[] = [10, 13]
  let sourceDone: bool = false
  let skipLeadingLf: bool = false

  loadNextChunk(): bool {
    if sourceDone {
      return false
    }

    if !source.next() {
      sourceDone = true
      return false
    }
    chunk := source.value()

    current = BlobReader(chunk)
    return true
  }

  skipLeadingLineFeed(): none {
    if !skipLeadingLf || current.remaining() == 0L {
      return
    }

    nextPosition := current.getPosition()
    if current.readByte() != 10 {
      current.setPosition(nextPosition)
    }
    skipLeadingLf = false
  }

  finishPendingLine(): string {
    lineBytes := pendingLine.build()
    lineReader := BlobReader(lineBytes)
    return lineReader.readString(lineReader.remaining())
  }

  flushTrailingLine(): string | none {
    remaining := current.remaining()
    if remaining > 0L {
      if pendingLine.length() == 0L {
        return current.readString(remaining)
      }

      pendingLine.writeBytes(current.readBytes(remaining))
    }

    if pendingLine.length() == 0L {
      return none
    }

    return finishPendingLine()
  }

  tryTakeCurrentLine(): string | none {
    if current.remaining() == 0L {
      return none
    }

    startPosition := current.getPosition()
    delimiterIndex := current.findNextAny(lineBreakBytes) else {
      return none
    }

    lineLength := delimiterIndex - startPosition
    if pendingLine.length() == 0L {
      line := current.readString(lineLength)
      if current.readByte() == 13 {
        skipLeadingLf = true
      }
      return line
    }

    if lineLength > 0L {
      pendingLine.writeBytes(current.readBytes(lineLength))
    }

    line := finishPendingLine()
    if current.readByte() == 13 {
      skipLeadingLf = true
    }

    return line
  }

  moveCurrentRemainderToPending(): none {
    remaining := current.remaining()
    if remaining > 0L {
      pendingLine.writeBytes(current.readBytes(remaining))
    }
  }

  next(): bool {
    while true {
      skipLeadingLineFeed()

      candidate := tryTakeCurrentLine()
      if candidate != none {
        currentValue = candidate
        return true
      }

      if sourceDone {
        trailing := flushTrailingLine()
        if trailing == none {
          return false
        }
        currentValue = trailing
        return true
      }

      moveCurrentRemainderToPending()

      if !loadNextChunk() {
        trailing := flushTrailingLine()
        if trailing == none {
          return false
        }
        currentValue = trailing
        return true
      }
    }
  }

  value(): string => currentValue!
}

export function blobStreamToLineStream(source: Stream<readonly byte[]>): Stream<string> {
  let stream: Stream<string> = DecodedLineStream {
    source,
  }
  return stream
}
