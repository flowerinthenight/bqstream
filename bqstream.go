package bqstream

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync/atomic"

	"cloud.google.com/go/bigquery"
)

type Option interface {
	Apply(*BqStream)
}

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *BqStream) { o.logger = w.l }

func WithLogger(v *log.Logger) Option { return withLogger{v} }

type BqStream struct {
	logger    *log.Logger
	projectId string
	region    string
	dataset   string
	table     string
	client    *bigquery.Client
	inserter  *bigquery.Inserter
	started   int32
	wq        chan interface{}
	wqdone    chan struct{}
	wqcnt     int
}

func (v *BqStream) worker(id int) {
	defer func() {
		v.logger.Printf("worker %v stopped", id)
		v.wqdone <- struct{}{}
	}()

	for j := range v.wq {
		err := v.inserter.Put(context.Background(), j)
		if err != nil {
			v.logger.Printf("bqstream.Put failed: %v", err)
		}
	}
}

// Add puts records to the streamer. It expects a slice of structs that implements
// the bigquery.ValueSaver interface.
func (v *BqStream) Add(txns interface{}) {
	switch {
	case atomic.LoadInt32(&v.started) != 1:
		return
	default:
		v.wq <- txns
	}
}

type StartInput struct {
	StreamCount int
}

// Start sets up the streaming goroutines. Use the Add() function to write streaming data.
func (v *BqStream) Start(ctx context.Context, in ...StartInput) error {
	var err error
	v.client, err = bigquery.NewClient(ctx, v.projectId)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient failed: %w", err)
	}

	v.inserter = v.client.Dataset(v.dataset).Table(v.table).Inserter()

	if len(in) > 0 {
		if in[0].StreamCount > 0 {
			v.wqcnt = in[0].StreamCount
		}
	}

	v.logger.Printf("starting %v worker(s) for bqstream", v.wqcnt)

	v.wq = make(chan interface{}, v.wqcnt)
	v.wqdone = make(chan struct{}, v.wqcnt)
	for i := 0; i < v.wqcnt; i++ {
		go v.worker(i)
	}

	atomic.StoreInt32(&v.started, 1)
	return nil
}

// Close stops all streaming goroutines and closes the BigQuery connection.
func (v *BqStream) Close() {
	if atomic.LoadInt32(&v.started) != 1 {
		return
	}

	atomic.StoreInt32(&v.started, 0)
	v.client.Close()
	close(v.wq)
	for i := 0; i < v.wqcnt; i++ {
		<-v.wqdone
	}
}

// New returns a BigQuery streaming object.
func New(project, region, dataset, table string, opts ...Option) *BqStream {
	v := &BqStream{
		projectId: project,
		region:    region,
		dataset:   dataset,
		table:     table,
		wqcnt:     runtime.NumCPU(),
	}

	for _, opt := range opts {
		opt.Apply(v)
	}

	if v.logger == nil {
		v.logger = log.New(os.Stdout, "[bqstream] ", 0)
	}

	return v
}
