'use strict';

/*
 * Kafka engine for artillery v2.
 *
 * config:
 *   engines: { kafka: {} }
 *   kafka:
 *     brokers: ["host:9092", ...]        (or target: "kafka://host:9092,host2:9092")
 *     clientId: predator-runner          (optional)
 *     ssl: true                          (optional)
 *     sasl: { mechanism, username, password }   (optional)
 *     producerPool: 4                    (optional, concurrent producers per worker)
 *     acks: 1                            (optional, kafkajs acks; default all)
 *
 * scenario:
 *   engine: kafka
 *   flow:
 *     - produce: { topic: orders, key: "{{ id }}", message: '{"id":"{{ id }}"}' }
 *     - think: 1
 *     - loop:                            (repeats nested steps `count` times)
 *         - produce: { ... }
 *         - think: 0.5
 *       count: 10
 *
 * Emits:
 *   kafka.messages_sent (counter), kafka.messages_sent.<topic> (counter),
 *   kafka.errors (counter),
 *   kafka.publish_latency (histogram, ms).
 * Consumer-lag monitoring lives in artillery-plugin-predator (leader-side).
 */

const { Kafka, logLevel } = require('kafkajs');

function kafkaConfig(script) {
    const cfg = Object.assign({}, script.config.kafka);
    if (!cfg.brokers) {
        const target = script.config.target || '';
        const hostList = target.replace(/^kafka:\/\//, '');
        cfg.brokers = hostList ? hostList.split(',') : [];
    }
    if (!cfg.brokers.length) {
        throw new Error('kafka engine: no brokers configured (config.kafka.brokers or a kafka:// target)');
    }
    return cfg;
}

function KafkaEngine(script, ee, helpers) {
    this.script = script;
    this.helpers = helpers;
    this.config = kafkaConfig(script);

    this.kafka = new Kafka({
        clientId: this.config.clientId || 'artillery-kafka-engine',
        brokers: this.config.brokers,
        ssl: this.config.ssl,
        sasl: this.config.sasl,
        logLevel: logLevel.NOTHING
    });

    // A small pool of producers per worker process — per-VU producers would
    // drown the broker in connections, but a single producer serializes acks
    // per partition (kafkajs awaits each send), capping one-partition
    // throughput at ~1/RTT msg/s. Round-robining a pool allows concurrent
    // in-flight sends to the same partition (ordering is irrelevant for load).
    this.poolSize = this.config.producerPool || 4;
    this.producers = [];
    this.nextProducer = 0;

}

KafkaEngine.prototype.getProducer = function () {
    const i = this.nextProducer;
    this.nextProducer = (this.nextProducer + 1) % this.poolSize;
    if (!this.producers[i]) {
        const producer = this.kafka.producer();
        this.producers[i] = producer.connect().then(() => producer);
    }
    return this.producers[i];
};


KafkaEngine.prototype.createScenario = function (scenarioSpec, ee) {
    const self = this;
    const tasks = scenarioSpec.flow.map((step) => self.step(step, ee));

    return function scenario(initialContext, callback) {
        ee.emit('started');
        let idx = 0;
        const next = (err, context) => {
            if (err) {
                return callback(err, context);
            }
            if (idx >= tasks.length) {
                return callback(null, context);
            }
            const task = tasks[idx++];
            task(context, next);
        };
        next(null, initialContext);
    };
};

KafkaEngine.prototype.step = function (step, ee) {
    const self = this;

    if (step.think !== undefined) {
        return this.helpers.createThink(step, this.script.config.defaults && this.script.config.defaults.think);
    }

    if (step.loop) {
        const tasks = step.loop.map((s) => self.step(s, ee));
        const count = step.count || 1;
        return function loopTask(context, callback) {
            let iteration = 0;
            let idx = 0;
            const next = (err, ctx) => {
                if (err) {
                    return callback(err, ctx);
                }
                if (idx >= tasks.length) {
                    idx = 0;
                    iteration++;
                }
                if (iteration >= count) {
                    return callback(null, ctx);
                }
                tasks[idx++](ctx, next);
            };
            next(null, context);
        };
    }

    if (step.produce) {
        return function produceTask(context, callback) {
            const params = step.produce;
            const topic = self.helpers.template(params.topic, context);
            const key = params.key !== undefined ? String(self.helpers.template(params.key, context)) : null;
            let value = self.helpers.template(params.message !== undefined ? params.message : params.json, context);
            if (typeof value === 'object') {
                value = JSON.stringify(value);
            }

            const startedAt = process.hrtime.bigint();
            self.getProducer()
                .then(producer => producer.send({ topic, messages: [{ key, value }], acks: self.config.acks }))
                .then(() => {
                    const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
                    ee.emit('counter', 'kafka.messages_sent', 1);
                    // Per-topic tally so a report can show which topics took the
                    // load, not just the total across all of them.
                    ee.emit('counter', `kafka.messages_sent.${topic}`, 1);
                    ee.emit('histogram', 'kafka.publish_latency', elapsedMs);
                    callback(null, context);
                })
                .catch(err => {
                    ee.emit('counter', 'kafka.errors', 1);
                    ee.emit('error', err.message || 'kafka produce error');
                    callback(err, context);
                });
        };
    }

    return function skip(context, callback) {
        callback(null, context);
    };
};

module.exports = KafkaEngine;
