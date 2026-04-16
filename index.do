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
