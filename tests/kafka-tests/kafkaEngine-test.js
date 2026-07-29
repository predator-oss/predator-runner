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
        blob.should.containEql('kafka.publish_latency');
        blob.should.containEql('kafka.consumer_lag_total.lagging-group');
        blob.should.containEql('kafka.consumer_lag_total.second-group');

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
});
