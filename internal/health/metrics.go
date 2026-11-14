package health

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type metrics struct {
	reg           *prometheus.Registry
	envelopes     *prometheus.CounterVec
	duplicates    prometheus.Counter
	rejected      prometheus.Counter
	unclaimed     prometheus.Counter
	missed        *prometheus.CounterVec
	delivered     *prometheus.CounterVec
	sends         *prometheus.CounterVec
	connected     prometheus.Gauge
	reconnects    prometheus.Counter
	lastMessage   prometheus.Gauge
	storeWritable prometheus.Gauge
	pruned        prometheus.Counter
	position      *prometheus.GaugeVec
}

func newMetrics() *metrics {
	m := &metrics{
		reg: prometheus.NewRegistry(),
		envelopes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "signal_hub_envelopes_total", Help: "Envelopes stored, by kind."}, []string{"kind"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "signal_hub_duplicates_total", Help: "Envelopes dropped as duplicates."}),
		rejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "signal_hub_frames_rejected_total", Help: "Upstream frames rejected."}),
		unclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "signal_hub_unclaimed_total", Help: "Messages no consumer claimed."}),
		missed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "signal_hub_missed_total", Help: "Messages missed by a consumer."}, []string{"consumer"}),
		delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "signal_hub_delivered_total", Help: "Messages delivered, by consumer and channel."},
			[]string{"consumer", "channel"}),
		sends: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "signal_hub_sends_total", Help: "Outbound sends, by consumer and result."},
			[]string{"consumer", "result"}),
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "signal_hub_upstream_connected", Help: "1 while the upstream WebSocket is connected."}),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "signal_hub_upstream_reconnects_total", Help: "Upstream reconnects."}),
		lastMessage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "signal_hub_last_message_timestamp_seconds", Help: "Unix time of the last stored message."}),
		storeWritable: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "signal_hub_store_writable", Help: "1 while the store accepts writes."}),
		pruned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "signal_hub_pruned_total", Help: "Messages pruned by retention."}),
		position: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "signal_hub_consumer_position", Help: "Last delivered seq per consumer and channel."},
			[]string{"consumer", "channel"}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.envelopes, m.duplicates, m.rejected, m.unclaimed, m.missed, m.delivered, m.sends,
		m.connected, m.reconnects, m.lastMessage, m.storeWritable, m.pruned, m.position,
	)
	return m
}
