package bqstream

import (
	"context"
	"fmt"
	"runtime"

	"cloud.google.com/go/bigquery"
	"github.com/golang/glog"
)

type Option interface {
	Apply(*BqStream)
}

type withProjectId string

func (w withProjectId) Apply(o *BqStream) { o.projectId = string(w) }

func WithProjectId(v string) Option { return withProjectId(v) }

type withRegion string

func (w withRegion) Apply(o *BqStream) { o.region = string(w) }

func WithRegion(v string) Option { return withRegion(v) }

type withDataset string

func (w withDataset) Apply(o *BqStream) { o.dataset = string(w) }

func WithDataset(v string) Option { return withDataset(v) }

type withTable string

func (w withTable) Apply(o *BqStream) { o.table = string(w) }

func WithTable(v string) Option { return withTable(v) }

type BqStream struct {
	projectId string
	region    string
	dataset   string
	table     string
	client    *bigquery.Client
	inserter  *bigquery.Inserter
	started   bool
	wq        chan interface{}
	wqdone    chan struct{}
	wqcnt     int
}

func (v *BqStream) worker(id int) {
	defer func() {
		glog.Infof("worker %v stopped", id)
		v.wqdone <- struct{}{}
	}()

	for j := range v.wq {
		err := v.inserter.Put(context.Background(), j)
		if err != nil {
			glog.Errorf("bqstream.Put failed: %v", err)
		}
	}
}

// Add puts records to the streamer. It expects a slice of structs that implements
// the bigquery.ValueSaver interface.
func (v *BqStream) Add(txns interface{}) {
	if !v.started {
		return
	}

	v.wq <- txns
}

type StartInput struct {
	StreamCount int
}

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

	glog.Infof("starting %v worker(s) for bqstream", v.wqcnt)
	v.wq = make(chan interface{}, v.wqcnt)
	v.wqdone = make(chan struct{}, v.wqcnt)
	for i := 0; i < v.wqcnt; i++ {
		go v.worker(i)
	}

	v.started = true
	return nil
}

func (v *BqStream) Close() {
	if !v.started {
		return
	}

	v.client.Close()
	close(v.wq)
	for i := 0; i < v.wqcnt; i++ {
		<-v.wqdone
	}
}

func New(opts ...Option) *BqStream {
	v := &BqStream{
		projectId: "mobingi-main",
		region:    "asia-northeast1",
		dataset:   "authd_txn",
		table:     "prod",
		wqcnt:     runtime.NumCPU(),
	}

	for _, opt := range opts {
		opt.Apply(v)
	}

	return v
}
