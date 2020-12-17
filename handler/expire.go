// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"encoding/json"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/pushgateway/storage"
	"github.com/robfig/cron/v3"
	"time"
)

type CronTask struct {
	cron        *cron.Cron
	logger      log.Logger
	MetricStore storage.MetricStore
}

func NewCronTask(
	l log.Logger,
	ms storage.MetricStore,
) *CronTask {

	if l == nil {
		l = log.NewNopLogger()
	}

	c := cron.New(cron.WithSeconds())

	return &CronTask{
		cron:        c,
		logger:      l,
		MetricStore: ms,
	}
}

func (cronTask CronTask) DeleteExpireMetric(seconds float64) {
	t := func() {
		timeNow := time.Now()
		familyMaps := cronTask.MetricStore.GetMetricFamiliesMap()
		for _, v := range familyMaps {
			labels := v.Labels
			if !v.LastPushSuccess() {
				cronTask.MetricStore.SubmitWriteRequest(storage.WriteRequest{
					Labels:    labels,
					Timestamp: time.Now(),
				})
				continue
			}
			for name, metricValues := range v.Metrics {
				createTimestamp := metricValues.Timestamp
				if name != "push_time_seconds" {
					continue
				}

				if timeNow.Sub(createTimestamp).Seconds() < seconds {
					continue
				}
				data, _ := json.Marshal(labels)
				labelsString := string(data)
				_ = cronTask.logger.Log("msg", "execute delete expire metric", "labels", labelsString)
				cronTask.MetricStore.SubmitWriteRequest(storage.WriteRequest{
					Labels:    labels,
					Timestamp: time.Now(),
				})
			}
		}
	}
	_, e := cronTask.cron.AddFunc("1 * * * * ?", t)
	if e != nil {
		panic(e)
	}
}

func (cronTask CronTask) Start() {
	cronTask.cron.Start()
}

func (cronTask CronTask) Stop() {
	cronTask.cron.Stop()
}
