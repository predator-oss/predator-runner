'use strict';

// Black-box: a real artillery v2 process drives the kafka engine against a
// real broker (KAFKA_BROKERS, default localhost:9092). Verifies produced
// messages by consuming them back, and that engine metrics and consumer-lag
// histograms reach the stats posted to (a mock) predator.

const should = require('should');
const http = require('node:http');
const { Kafka } = require('kafkajs');
const runner = require('../../app/models/runner');

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = 'engine-test';

const startServer = (handler) => new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => resolve(server));
});
const readJson = (req) => new Promise((resolve) => {
    let body = '';
    req.on('data', c => { body += c; });
    req.on('end', () => { try { resolve(body ? JSON.parse(body) : {}); } catch (e) { resolve(body); } });
});

describe('Kafka engine with real artillery and a real broker', function () {
    this.timeout(180000);

    let predator;
    let statsPosts;
    let currentTest;

    before(async () => {
        predator = await startServer(async (req, res) => {
            const json = (code, payload) => {
                res.writeHead(code, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(payload));
            };
            if (req.method === 'GET' && /^\/v1\/tests\/[^/]+$/.test(req.url)) return json(200, currentTest);
            if (req.method === 'POST' && req.url.endsWith('/subscribe')) { await readJson(req); return json(201, {}); }
            if (req.method === 'POST' && req.url.endsWith('/stats')) { statsPosts.push(await readJson(req)); return json(204, {}); }
            json(404, {});
        });
    });

    after(async () => predator.close());

    beforeEach(() => {
        statsPosts = [];
    });

    it('produces templated messages, reports engine metrics and consumer lag', async () => {
        // Per-partition lag only exists for groups with committed offsets —
        // pin lagging-group to offset 0 so every produced message counts as lag.
        const admin = new Kafka({ clientId: 'engine-test-admin', brokers: BROKERS }).admin();
        await admin.connect();
        await admin.createTopics({ topics: [{ topic: TOPIC }] }).catch(() => {});
        await admin.setOffsets({ groupId: 'lagging-group', topic: TOPIC, partitions: [{ partition: 0, offset: '0' }] }).catch(() => {});
        await admin.disconnect();

        currentTest = {
            id: 'kafka-test',
            name: 'kafka produce test',
            artillery_test: {
                config: {
                    target: `kafka://${BROKERS.join(',')}`,
                    phases: [{ duration: 12, arrivalRate: 3 }],
                    engines: { kafka: {} },
                    kafka: {
                        brokers: BROKERS,
                        lagMonitor: { consumerGroups: ['lagging-group', 'second-group'], intervalMs: 500 }
                    },
                    variables: { region: ['eu', 'us'] }
                },
                scenarios: [{
                    name: 'produce order',
                    engine: 'kafka',
                    flow: [{
                        produce: {
                            topic: TOPIC,
                            key: '{{ region }}',
                            message: '{"region": "{{ region }}", "source": "engine-test"}'
                        }
                    }]
                }]
            }
        };

        await runner.runTest({
            jobId: 'job-id', testId: 'kafka-test', reportId: 'report-id',
            containerId: 'kafka-runner-test', jobType: 'load_test',
            environment: 'test', duration: 12, arrivalRate: 3,
            httpPoolSize: 10, statsInterval: 30,
            predatorUrl: `http://127.0.0.1:${predator.address().port}/v1`
        });

        const statuses = statsPosts.map(p => p.phase_status);
        statuses.should.containEql('done');

        const intermediates = statsPosts.filter(p => p.phase_status === 'first_intermediate' || p.phase_status === 'intermediate');
        intermediates.length.should.be.above(0);
        const blob = JSON.stringify(intermediates);
        blob.should.containEql('kafka.messages_sent');
        blob.should.containEql(`kafka.messages_sent.${TOPIC}`);
        blob.should.containEql('kafka.publish_latency');
        blob.should.containEql('kafka.consumer_lag_total.lagging-group');
        blob.should.containEql('kafka.consumer_lag_total.second-group');
        blob.should.containEql(`kafka.consumer_lag_partition.lagging-group.${TOPIC}.0`);
        // predator buckets intermediates by timestamp — null collapses charts
        intermediates.forEach((p) => {
            should.exist(JSON.parse(p.data).timestamp, 'intermediate stats must carry a timestamp');
        });

        // Consume everything back and verify the payloads are real and templated.
        const kafka = new Kafka({ clientId: 'engine-test-verifier', brokers: BROKERS });
        const consumer = kafka.consumer({ groupId: `verifier-${Date.now()}` });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
        const messages = [];
        await new Promise((resolve) => {
            let settle = setTimeout(resolve, 8000);
            consumer.run({
                eachMessage: async ({ message }) => {
                    messages.push(JSON.parse(message.value.toString()));
                    clearTimeout(settle);
                    settle = setTimeout(resolve, 1500);
                }
            });
        });
        await consumer.disconnect();

        const mine = messages.filter(m => m.source === 'engine-test');
        mine.length.should.be.aboveOrEqual(15, `expected ~20 produced messages, consumed ${mine.length}`);
        new Set(mine.map(m => m.region)).size.should.be.aboveOrEqual(2, 'variable templating should produce both regions');
    });

    it('loops produce steps, uses a producer pool with acks=1, and reports sends as rps/latency', async () => {
        currentTest = {
            id: 'loop-test',
            name: 'kafka loop test',
            artillery_test: {
                config: {
                    target: `kafka://${BROKERS.join(',')}`,
                    phases: [{ duration: 5, arrivalRate: 2 }],
                    engines: { kafka: {} },
                    kafka: { brokers: BROKERS, producerPool: 2, acks: 1 }
                },
                scenarios: [{
                    name: 'burst',
                    engine: 'kafka',
                    flow: [{
                        loop: [
                            { produce: { topic: TOPIC, key: 'loop', message: '{"source": "loop-test"}' } },
                            { think: 0.1 }
                        ],
                        count: 3
                    }]
                }]
            }
        };

        await runner.runTest({
            jobId: 'job-id', testId: 'loop-test', reportId: 'report-id',
            containerId: 'loop-runner-test', jobType: 'load_test',
            environment: 'test', duration: 5, arrivalRate: 2,
            httpPoolSize: 10, statsInterval: 30,
            predatorUrl: `http://127.0.0.1:${predator.address().port}/v1`
        });

        statsPosts.map(p => p.phase_status).should.containEql('done');

        // kafka-only windows must surface sends in the fields predator charts:
        // requestsCompleted/rps from messages_sent, latency from publish_latency.
        const intermediates = statsPosts.filter(p => p.phase_status === 'first_intermediate' || p.phase_status === 'intermediate');
        const withSends = intermediates.map(p => JSON.parse(p.data)).filter(r => ((r.counters || {})['kafka.messages_sent'] || 0) > 0);
        const sent = withSends.reduce((sum, r) => sum + r.counters['kafka.messages_sent'], 0);
        // ~10 VUs x 3 loop iterations - loop must multiply produces
        sent.should.be.aboveOrEqual(12, `loop should produce 3x per VU, sent ${sent}`);
        withSends.length.should.be.above(0);
        withSends.forEach((r) => {
            r.requestsCompleted.should.be.aboveOrEqual(r.counters['kafka.messages_sent']);
            r.rps.count.should.be.aboveOrEqual(r.counters['kafka.messages_sent']);
            should.exist(r.latency.median, 'latency should be mapped from kafka.publish_latency');
        });
    });

    it('runs a mixed script: http scenario and kafka scenario side by side', async () => {
        let httpHits = 0;
        const api = await startServer((req, res) => {
            httpHits++;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end('{"ok":true}');
        });

        currentTest = {
            id: 'mixed-test',
            name: 'mixed http+kafka',
            artillery_test: {
                config: {
                    target: `http://127.0.0.1:${api.address().port}`,
                    phases: [{ duration: 10, arrivalRate: 4 }],
                    engines: { kafka: {} },
                    kafka: {
                        brokers: BROKERS,
                        lagMonitor: { consumerGroups: ['lagging-group'], intervalMs: 500 }
                    }
                },
                scenarios: [
                    { name: 'http flow', weight: 1, flow: [{ get: { url: '/health' } }] },
                    {
                        name: 'kafka flow',
                        weight: 1,
                        engine: 'kafka',
                        flow: [{ produce: { topic: TOPIC, key: 'mix', message: '{"source": "mixed-test"}' } }]
                    }
                ]
            }
        };

        try {
            await runner.runTest({
                jobId: 'job-id', testId: 'mixed-test', reportId: 'report-id',
                containerId: 'mixed-runner-test', jobType: 'load_test',
                environment: 'test', duration: 10, arrivalRate: 4,
                httpPoolSize: 10, statsInterval: 30,
                predatorUrl: `http://127.0.0.1:${predator.address().port}/v1`
            });
        } finally {
            api.close();
        }

        statsPosts.map(p => p.phase_status).should.containEql('done');
        const blob = JSON.stringify(statsPosts);
        httpHits.should.be.above(0, 'http scenario should have hit the api');
        blob.should.containEql('kafka.messages_sent');
        blob.should.containEql('kafka.consumer_lag_total.lagging-group');
    });
});
