'use strict';

// Black-box tests of the artillery v2 run pipeline: a real artillery process
// runs short scenarios against a local hapi target, while a mock predator
// captures everything the runner and the predator plugin post back. No stubs —
// if artillery v2 breaks csv/processor/expect handling, these fail.

const should = require('should');
const http = require('node:http');
const runner = require('../../../app/models/runner');

// Plain node:http mocks — @hapi/hapi 18 hangs on POST payloads under modern
// Node, which is exactly the kind of legacy this suite exists to retire.
const startServer = (handler) => new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => resolve(server));
});
const readJson = (req) => new Promise((resolve) => {
    let body = '';
    req.on('data', c => { body += c; });
    req.on('end', () => { try { resolve(body ? JSON.parse(body) : {}); } catch (e) { resolve(body); } });
});

describe('Run test with real artillery v2', function () {
    this.timeout(120000);

    let target;
    let predator;
    let targetRequests;
    let statsPosts;
    let currentTest;

    const jobConfigFor = (overrides = {}) => Object.assign({
        jobId: 'job-id',
        testId: 'test-id',
        reportId: 'report-id',
        containerId: 'runner-under-test',
        jobType: 'load_test',
        environment: 'test',
        duration: 2,
        arrivalRate: 3,
        httpPoolSize: 100,
        statsInterval: 30,
        predatorUrl: null // filled in beforeEach
    }, overrides);

    const baseTest = () => ({
        id: 'test-id',
        name: 'v2 pipeline test',
        artillery_test: {
            config: {
                target: null, // filled per test
                phases: [{ duration: 2, arrivalRate: 3 }]
            },
            scenarios: [{
                name: 'hit target',
                flow: [{ post: { url: '/data', json: { token: 'predator-rules' } } }]
            }]
        }
    });

    before(async () => {
        target = await startServer(async (req, res) => {
            targetRequests.push({ path: req.url, payload: await readJson(req), headers: req.headers });
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end('{"ok":true}');
        });

        predator = await startServer(async (req, res) => {
            const json = (code, payload) => {
                res.writeHead(code, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(payload));
            };
            if (req.method === 'GET' && /^\/v1\/tests\/[^/]+$/.test(req.url)) {
                return json(200, currentTest);
            }
            if (req.method === 'GET' && req.url.startsWith('/v1/files/')) {
                res.writeHead(200, { 'Content-Type': 'text/plain' });
                return res.end(currentTest.__file || '');
            }
            if (req.method === 'GET' && req.url.startsWith('/v1/processors/')) {
                return json(200, { javascript: currentTest.__processor });
            }
            if (req.method === 'POST' && req.url.endsWith('/subscribe')) {
                await readJson(req);
                return json(201, {});
            }
            if (req.method === 'POST' && req.url.endsWith('/stats')) {
                statsPosts.push(await readJson(req));
                return json(204, {});
            }
            json(404, { message: 'not found' });
        });
    });

    after(async () => {
        target.close();
        predator.close();
    });

    beforeEach(() => {
        targetRequests = [];
        statsPosts = [];
        currentTest = baseTest();
        currentTest.artillery_test.config.target = `http://127.0.0.1:${target.address().port}`;
    });

    const runWith = async (overrides) => {
        const jobConfig = jobConfigFor(Object.assign({
            predatorUrl: `http://127.0.0.1:${predator.address().port}/v1`
        }, overrides));
        await runner.runTest(jobConfig);
        return jobConfig;
    };

    const phaseStatuses = () => statsPosts.map(p => p.phase_status);

    it('runs a load test and reports the full phase lifecycle with legacy-shaped stats', async () => {
        await runWith();

        phaseStatuses().should.containEql('started_phase');
        phaseStatuses().should.containEql('done');
        const intermediate = statsPosts.find(p => p.phase_status === 'first_intermediate' || p.phase_status === 'intermediate');
        should.exist(intermediate, 'expected at least one intermediate stats post');

        const report = JSON.parse(intermediate.data);
        report.should.have.properties(['scenariosCreated', 'requestsCompleted', 'latency', 'rps', 'codes']);
        report.codes.should.have.property('200');
        report.rps.mean.should.be.above(0);

        targetRequests.length.should.be.above(0);
        targetRequests.every(r => r.payload && r.payload.token === 'predator-rules').should.eql(true);
    });

    it('runs custom javascript processors (beforeRequest hook reaches the wire)', async () => {
        currentTest.processor_id = 'processor-under-test';
        currentTest.__processor = [
            'module.exports.signRequest = function (requestParams, context, ee, next) {',
            "  requestParams.headers = requestParams.headers || {};",
            "  requestParams.headers['x-signature'] = 'signed-by-processor';",
            '  return next();',
            '};'
        ].join('\n');
        currentTest.artillery_test.scenarios[0].flow[0].post.beforeRequest = 'signRequest';

        await runWith();

        phaseStatuses().should.containEql('done');
        targetRequests.length.should.be.above(0);
        targetRequests.every(r => r.headers['x-signature'] === 'signed-by-processor').should.eql(true);
    });

    it('feeds csv payload variables into requests', async () => {
        currentTest.csv_file_id = 'csv-under-test';
        currentTest.__file = 'username,city\nmickey,tel aviv\ndonald,haifa\n';
        currentTest.artillery_test.scenarios[0].flow[0].post.json = { user: '{{ username }}', city: '{{ city }}' };

        await runWith();

        phaseStatuses().should.containEql('done');
        targetRequests.length.should.be.above(0);
        const users = new Set(targetRequests.map(r => r.payload.user));
        [...users].every(u => ['mickey', 'donald'].includes(u)).should.eql(true, `unexpected users: ${[...users]}`);
    });

    it('runs functional tests with expectations', async () => {
        currentTest.artillery_test.scenarios[0].flow[0].post.expect = [{ statusCode: 200 }];

        await runWith({ jobType: 'functional_test', arrivalCount: 5, arrivalRate: undefined });

        phaseStatuses().should.containEql('done');
        targetRequests.length.should.eql(5);
    });

    it('does not double-report with multiple workers', async () => {
        process.env.WORKERS = '2';
        try {
            await runWith();
        } finally {
            delete process.env.WORKERS;
        }

        phaseStatuses().filter(p => p === 'done').length.should.eql(1, 'done must post exactly once');
        const reported = statsPosts
            .filter(p => p.phase_status === 'first_intermediate' || p.phase_status === 'intermediate')
            .reduce((sum, p) => sum + JSON.parse(p.data).requestsCompleted, 0);
        reported.should.eql(targetRequests.length, 'reported requests must equal actual requests');
    });

    it('processors can require modules bundled with the runner', async () => {
        currentTest.processor_id = 'processor-with-require';
        currentTest.__processor = [
            "const { v4: uuid } = require('uuid');",
            'module.exports.tagRequest = function (requestParams, context, ee, next) {',
            "  requestParams.headers = requestParams.headers || {};",
            "  requestParams.headers['x-request-id'] = uuid();",
            '  return next();',
            '};'
        ].join('\n');
        currentTest.artillery_test.scenarios[0].flow[0].post.beforeRequest = 'tagRequest';

        await runWith();

        phaseStatuses().should.containEql('done');
        targetRequests.length.should.be.above(0);
        targetRequests.every(r => /^[0-9a-f-]{36}$/.test(r.headers['x-request-id'])).should.eql(true);
    });

    it('rejects when the test file cannot be fetched', async () => {
        currentTest = null; // GET /tests/:id returns empty -> connector fails
        await runWith().should.be.rejected();
    });

    it('rejects when the csv file cannot be parsed', async () => {
        currentTest.csv_file_id = 'bad-csv';
        currentTest.__file = 'a,b\n"unterminated\n';
        await runWith().should.be.rejectedWith(/Failure to parse csv/);
    });
});
