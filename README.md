![main](https://github.com/flowerinthenight/bqstream/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/bqstream.svg)](https://pkg.go.dev/github.com/flowerinthenight/bqstream)

**NOTE: BigQuery recommends using its [Storage Write API](https://cloud.google.com/bigquery/docs/write-api) instead of its legacy streaming API which this library uses.**

## bqstream

A simple wrapper library for streaming data to BigQuery. By default, it will create streaming goroutines equal to the number of cores it's running on.

At the moment, authentication is done by providing the following environment variable:
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account/file.json
```

## Usage

To use the library, you can do something like this:

```go
// The data that we will be streaming to BigQuery.
type Data struct {
	Key   string
	Value string
}

// Save implements the ValueSaver interface.
func (d *Data) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"key":   d.Key,
		"value": d.Value,
	}, "", nil
}

// Create a streamer for a table in Tokyo region.
streamer = bqstream.New("project01", "asia-northeast1", "dataset01", "table01")

// Stream the data.
streamer.Add([]*Data{
	&Data{
		Key:   "samplekey",
		Value: "samplevalue",
	},
})
```
