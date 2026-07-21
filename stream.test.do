import { decodeChunkLines } from "./test_support"

export function testBlobStreamToLineStreamHandlesMixedLineEndingsAcrossChunks(): none {
  lines := decodeChunkLines(["alpha\r", "\nbe", "ta\n", "\rgam", "ma\rde", "lta"])

  assert(lines.length == 5, "expected five decoded lines")
  assert(lines[0] == "alpha", "expected CRLF across chunks to decode one line")
  assert(lines[1] == "beta", "expected LF-terminated lines to decode")
  assert(lines[2] == "", "expected a standalone CR to produce an empty line")
  assert(lines[3] == "gamma", "expected CR-terminated lines to decode")
  assert(lines[4] == "delta", "expected a trailing unterminated line to be emitted")
}

export function testBlobStreamToLineStreamEmitsTrailingLineWithoutDelimiter(): none {
  lines := decodeChunkLines(["one\n", "two", "", "\rthree"])

  assert(lines.length == 3, "expected the trailing line to be emitted once the source ends")
  assert(lines[0] == "one", "expected LF-delimited lines to decode")
  assert(lines[1] == "two", "expected CR-delimited content to decode")
  assert(lines[2] == "three", "expected the trailing line without a delimiter to be emitted")
}