import { BlobReader, BlobBuilder } from "std/blob"

class FilteredStream<T> implements Stream<T> {
  source: Stream<T>
  pred: (it: T): bool

  next(): T | null {
    while true {
      candidate := this.source.next() else {
          return null
      }
      
      if (this.pred(candidate)) {
          return candidate
      }
    }
  }
}

class MappedStream<T, U> implements Stream<U> {
  source: Stream<T>
  transform: (it: T): U

  next(): U | null {
    value := this.source.next() else {
      return null
    }
    return this.transform(value)
  }
}

class TakeStream<T> implements Stream<T> {
  source: Stream<T>
  remaining: int

  next(): T | null {
    if this.remaining <= 0 {
      return null
    }
    value := this.source.next()
    if value == null {
      return null
    }
    this.remaining = this.remaining - 1
    return value
  }
}

export class Chain<T> implements Stream<T> {
  source: Stream<T>

  next(): T | null => this.source.next()

  filter(pred: (it: T): bool): Chain<T> => Chain<T> { source: FilteredStream<T> { source: this.source, pred } }
  map<U>(transform: (it: T): U): Chain<U> => Chain<U> { source: MappedStream<T, U> { source: this.source, transform } }
  take(count: int): Chain<T> => Chain<T> { source: TakeStream<T> { source: this.source, remaining: count } }

  collect(): T[] {
    let values: T[] = []
    for item of this.source {
      values.push(item)
    }
    return values
  }
}

class DecodedLineStream implements Stream<string> {
  source: Stream<readonly byte[]>
  pendingLine: BlobBuilder = BlobBuilder()
  current: BlobReader = BlobReader([])
  lineBreakBytes: readonly byte[] = [10, 13]
  sourceDone: bool = false
  skipLeadingLf: bool = false

  loadNextChunk(): bool {
    if this.sourceDone {
      return false
    }

    chunk := this.source.next() else {
      this.sourceDone = true
      return false
    }

    this.current = BlobReader(chunk)
    return true
  }

  skipLeadingLineFeed(): void {
    if !this.skipLeadingLf || this.current.remaining() == 0L {
      return
    }

    nextPosition := this.current.getPosition()
    if this.current.readByte() != 10 {
      this.current.setPosition(nextPosition)
    }
    this.skipLeadingLf = false
  }

  finishPendingLine(): string {
    lineBytes := this.pendingLine.build()
    lineReader := BlobReader(lineBytes)
    return lineReader.readString(lineReader.remaining())
  }

  flushTrailingLine(): string | null {
    remaining := this.current.remaining()
    if remaining > 0L {
      if this.pendingLine.length() == 0L {
        return this.current.readString(remaining)
      }

      this.pendingLine.writeBytes(this.current.readBytes(remaining))
    }

    if this.pendingLine.length() == 0L {
      return null
    }

    return this.finishPendingLine()
  }

  tryTakeCurrentLine(): string | null {
    if this.current.remaining() == 0L {
      return null
    }

    startPosition := this.current.getPosition()
    delimiterIndex := this.current.findNextAny(this.lineBreakBytes) else {
      return null
    }

    lineLength := delimiterIndex - startPosition
    if this.pendingLine.length() == 0L {
      line := this.current.readString(lineLength)
      if this.current.readByte() == 13 {
        this.skipLeadingLf = true
      }
      return line
    }

    if lineLength > 0L {
      this.pendingLine.writeBytes(this.current.readBytes(lineLength))
    }

    line := this.finishPendingLine()
    if this.current.readByte() == 13 {
      this.skipLeadingLf = true
    }

    return line
  }

  moveCurrentRemainderToPending(): void {
    remaining := this.current.remaining()
    if remaining > 0L {
      this.pendingLine.writeBytes(this.current.readBytes(remaining))
    }
  }

  next(): string | null {
    while true {
      this.skipLeadingLineFeed()

      candidate := this.tryTakeCurrentLine()
      if candidate != null {
        return candidate
      }

      if this.sourceDone {
        return this.flushTrailingLine()
      }

      this.moveCurrentRemainderToPending()

      if !this.loadNextChunk() {
        return this.flushTrailingLine()
      }
    }
  }
}

export function blobStreamToLineStream(source: Stream<readonly byte[]>): Stream<string> {
  let stream: Stream<string> = DecodedLineStream {
    source,
  }
  return stream
}
