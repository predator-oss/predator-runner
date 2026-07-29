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
 *
 * scenario:
 *   engine: kafka
 *   flow:
 *     - produce: { topic: orders, key: "{{ id }}", message: '{"id":"{{ id }}"}' }
 *     - think: 1
 *
 * Emits:
 *   kafka.messages_sent (counter), kafka.errors (counter),
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

    // One producer per worker process — per-VU producers would drown the
    // broker in connections instead of load.
    this.producerReady = null;

}

KafkaEngine.prototype.getProducer = function () {
    if (!this.producerReady) {
        const producer = this.kafka.producer();
        this.producerReady = producer.connect().then(() => producer);
    }
    return this.producerReady;
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
                .then(producer => producer.send({ topic, messages: [{ key, value }] }))
                .then(() => {
                    const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
                    ee.emit('counter', 'kafka.messages_sent', 1);
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
