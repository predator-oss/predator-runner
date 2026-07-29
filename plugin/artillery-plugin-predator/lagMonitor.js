'use strict';

// Leader-side consumer-group lag poller. Living in the plugin makes it
// exactly-once by construction (workers each load plugins too, but only the
// leader reports), and lets lag reach predator without relying on artillery's
// worker metric bus. Samples are summarised per stats window in the same
// shape artillery uses for histograms.
//
// Monitors one or more consumer groups; each group gets its own metric names
// (kafka.consumer_lag_total.<group> / kafka.consumer_lag_max_partition.<group>)
// so the report can draw a line per group.

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

const groupsFrom = (lagMonitor) => {
    // Accept consumerGroups: [...] (new) or consumerGroup: "x" (back-compat).
    if (Array.isArray(lagMonitor.consumerGroups) && lagMonitor.consumerGroups.length) {
        return lagMonitor.consumerGroups;
    }
    return lagMonitor.consumerGroup ? [lagMonitor.consumerGroup] : [];
};

class LagMonitor {
    constructor (kafkaConfig) {
        const { Kafka, logLevel } = require('kafkajs');
        this.config = kafkaConfig;
        this.groups = groupsFrom(kafkaConfig.lagMonitor);
        this.kafka = new Kafka({
            clientId: 'predator-lag-monitor',
            brokers: kafkaConfig.brokers,
            ssl: kafkaConfig.ssl,
            sasl: kafkaConfig.sasl,
            logLevel: logLevel.NOTHING
        });
        // Per-group sample buffers: { group: { totals: [], maxes: [], partitions: { 'topic.0': [] } } }
        this.samples = {};
        this.groups.forEach((g) => { this.samples[g] = { totals: [], maxes: [], partitions: {} }; });
        this.errors = 0;
    }

    start () {
        const { intervalMs = 5000 } = this.config.lagMonitor;
        this.admin = this.kafka.admin();

        const pollGroup = async (group) => {
            const offsets = await this.admin.fetchOffsets({ groupId: group });
            let total = 0;
            let maxPartition = 0;
            const perPartition = [];
            for (const topicOffsets of offsets) {
                const latest = await this.admin.fetchTopicOffsets(topicOffsets.topic);
                for (const p of topicOffsets.partitions) {
                    const head = latest.find(l => l.partition === p.partition);
                    if (!head) continue;
                    const committed = p.offset === '-1' ? 0 : Number(p.offset);
                    const lag = Math.max(0, Number(head.offset) - committed);
                    total += lag;
                    maxPartition = Math.max(maxPartition, lag);
                    perPartition.push({ topic: topicOffsets.topic, partition: p.partition, lag });
                }
            }
            const buf = this.samples[group];
            buf.totals.push(total);
            buf.maxes.push(maxPartition);
            for (const pp of perPartition) {
                const key = `${pp.topic}.${pp.partition}`;
                (buf.partitions[key] = buf.partitions[key] || []).push(pp.lag);
            }
            this.pushToPrometheus(group, total, maxPartition, perPartition);
        };

        const poll = async () => {
            for (const group of this.groups) {
                try {
                    await pollGroup(group);
                } catch (err) {
                    this.errors++;
                }
            }
        };

        this.ready = this.admin.connect().then(() => {
            poll();
            this.timer = setInterval(poll, intervalMs);
            this.timer.unref();
        }).catch(() => { this.errors++; });
    }

    // Grafana path: per-group gauges, group as a label.
    pushToPrometheus (group, total, maxPartition, perPartition = []) {
        if (process.env.METRICS_PLUGIN_NAME !== 'prometheus' || !process.env.METRICS_EXPORT_CONFIG) return;
        try {
            const cfg = JSON.parse(Buffer.from(process.env.METRICS_EXPORT_CONFIG, 'base64').toString('ascii'));
            if (!cfg.push_gateway_url) return;
            const runId = process.env.REPORT_ID || 'unknown';
            const base = cfg.push_gateway_url.replace(/\/$/, '');
            const body = 'kafka_consumer_lag_total ' + total + '\n' +
                         'kafka_consumer_lag_max_partition ' + maxPartition + '\n' +
                         perPartition.map(p => `kafka_consumer_lag_partition{topic="${p.topic}",partition="${p.partition}"} ${p.lag}`).join('\n') + (perPartition.length ? '\n' : '');
            fetch(base + '/metrics/job/predator_kafka_lag/test_run_id/' + encodeURIComponent(runId) + '/consumer_group/' + encodeURIComponent(group), {
                method: 'PUT',
                headers: { 'Content-Type': 'text/plain' },
                body
            }).catch(() => { this.errors++; });
        } catch (e) { this.errors++; }
    }

    // Drain the window: per-group summaries for the stats post, then reset.
    drain () {
        const out = {};
        for (const group of this.groups) {
            const buf = this.samples[group];
            const totals = summarize(buf.totals);
            const maxes = summarize(buf.maxes);
            if (totals) out[`kafka.consumer_lag_total.${group}`] = totals;
            if (maxes) out[`kafka.consumer_lag_max_partition.${group}`] = maxes;
            for (const [pKey, lags] of Object.entries(buf.partitions)) {
                const s = summarize(lags);
                if (s) out[`kafka.consumer_lag_partition.${group}.${pKey}`] = s;
            }
            buf.totals = [];
            buf.maxes = [];
            buf.partitions = {};
        }
        return out;
    }

    async stop () {
        if (this.timer) clearInterval(this.timer);
        if (this.admin) await this.admin.disconnect().catch(() => {});
    }
}

module.exports = { LagMonitor };
