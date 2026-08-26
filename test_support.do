import { BlobBuilder } from "std/blob"
import { blobStreamToLineStream } from "./index"

function encodeChunk(text: string): readonly byte[] {
  builder := BlobBuilder()
  builder.writeString(text)
  return builder.build()
}

class EncodedChunkStream implements Stream<readonly byte[]> {
  chunks: readonly string[]
  let index: int = 0
  let currentValue: readonly byte[] | none = none

  next(): bool {
    if this.index >= this.chunks.length {
      return false
    }

    this.currentValue = encodeChunk(this.chunks[this.index])
    this.index = this.index + 1
    return true
  }

  value(): readonly byte[] => this.currentValue!
}

function collectLines(stream: Stream<string>): string[] {
  lines: string[] := []
  for line of stream {
    lines.push(line)
  }
  return lines
}

export function decodeChunkLines(chunks: readonly string[]): string[] {
  let source: Stream<readonly byte[]> = EncodedChunkStream { chunks }
  return collectLines(blobStreamToLineStream(source))
}
