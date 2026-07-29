'use strict';

// Leader-side consumer-group lag poller. Living in the plugin makes it
// exactly-once by construction (workers each load plugins too, but only the
// leader reports), and lets lag reach predator without relying on artillery's
// worker metric bus. Samples are summarised per stats window in the same
// shape artillery uses for histograms.

const summarize = (samples) => {
    if (!samples.length) return null;
    const s = [...samples].sort((a, b) => a - b);
    const q = (p) => s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
    return {
        min: s[0], max: s[s.length - 1], count: s.length,
        mean: Math.round((s.reduce((a, b) => a + b, 0) / s.length) * 10) / 10,
        p50: q(50), median: q(50), p75: q(75), p90: q(90), p95: q(95), p99: q(99), p999: q(99.9)
    };
};

class LagMonitor {
    constructor (kafkaConfig) {
        const { Kafka, logLevel } = require('kafkajs');
        this.config = kafkaConfig;
        this.kafka = new Kafka({
            clientId: 'predator-lag-monitor',
            brokers: kafkaConfig.brokers,
            ssl: kafkaConfig.ssl,
            sasl: kafkaConfig.sasl,
            logLevel: logLevel.NOTHING
        });
        this.totals = [];
        this.maxes = [];
        this.errors = 0;
    }

    start () {
        const { consumerGroup, intervalMs = 5000 } = this.config.lagMonitor;
        this.admin = this.kafka.admin();
        const poll = async () => {
            try {
                const offsets = await this.admin.fetchOffsets({ groupId: consumerGroup });
                let total = 0;
                let maxPartition = 0;
                for (const topicOffsets of offsets) {
                    const latest = await this.admin.fetchTopicOffsets(topicOffsets.topic);
                    for (const p of topicOffsets.partitions) {
                        const head = latest.find(l => l.partition === p.partition);
                        if (!head) continue;
                        const committed = p.offset === '-1' ? 0 : Number(p.offset);
                        const lag = Math.max(0, Number(head.offset) - committed);
                        total += lag;
                        maxPartition = Math.max(maxPartition, lag);
                    }
                }
                this.totals.push(total);
                this.maxes.push(maxPartition);
                this.pushToPrometheus(total, maxPartition);
            } catch (err) {
                this.errors++;
            }
        };
        this.ready = this.admin.connect().then(() => {
            poll();
            this.timer = setInterval(poll, intervalMs);
            this.timer.unref();
        }).catch(() => { this.errors++; });
    }

    // Grafana path: when the job exports to prometheus, lag rides along as
    // gauges on the same pushgateway the runner's other metrics use.
    pushToPrometheus (total, maxPartition) {
        if (process.env.METRICS_PLUGIN_NAME !== 'prometheus' || !process.env.METRICS_EXPORT_CONFIG) return;
        try {
            const cfg = JSON.parse(Buffer.from(process.env.METRICS_EXPORT_CONFIG, 'base64').toString('ascii'));
            if (!cfg.push_gateway_url) return;
            const runId = process.env.REPORT_ID || 'unknown';
            const base = cfg.push_gateway_url.replace(/\/$/, '');
            const body = 'kafka_consumer_lag_total ' + total + '\n' +
                         'kafka_consumer_lag_max_partition ' + maxPartition + '\n';
            fetch(base + '/metrics/job/predator_kafka_lag/test_run_id/' + encodeURIComponent(runId), {
                method: 'PUT',
                headers: { 'Content-Type': 'text/plain' },
                body
            }).catch(() => { this.errors++; });
        } catch (e) { this.errors++; }
    }

    // Drain the window: summaries for the stats post, then reset.
    drain () {
        const out = {};
        const totals = summarize(this.totals);
        const maxes = summarize(this.maxes);
        if (totals) out['kafka.consumer_lag_total'] = totals;
        if (maxes) out['kafka.consumer_lag_max_partition'] = maxes;
        this.lastDrained = out;
        this.totals = [];
        this.maxes = [];
        return out;
    }

    async stop () {
        if (this.timer) clearInterval(this.timer);
        if (this.admin) await this.admin.disconnect().catch(() => {});
    }
}

module.exports = { LagMonitor };
