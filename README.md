![Main](https://github.com/flowerinthenight/bqstream/workflows/Main/badge.svg)

## bqstream

A simple wrapper library for streaming data to BigQuery.

## Usage

To use the library, you can do something like this:

```go
// Create a streamer for a table in Tokyo region
streamer = bqstream.New("project01", "asia-northeast1", "dataset01", "table01")
```
