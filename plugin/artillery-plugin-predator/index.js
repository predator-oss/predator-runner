'use strict';

// Runs inside the artillery v2 process (config.plugins.predator). Translates
// v2 events to the stats contract predator has always consumed:
//   phaseStarted -> started_phase, stats -> first_intermediate/intermediate,
//   done -> done. stats.report() emits the artillery v1 legacy report shape,
// so no field mapping is needed.
//
// Job context arrives via env because the plugin lives in a separate process
// from the predator-runner supervisor that spawned artillery.

const CONTEXT = {
    predatorUrl: process.env.PREDATOR_URL,
    testId: process.env.TEST_ID,
    reportId: process.env.REPORT_ID,
    runnerId: process.env.RUNNER_ID
};

const MAX_ATTEMPTS = 3;
const BACKOFF_BASE_MS = 1000;
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function postStats(body) {
    const url = `${CONTEXT.predatorUrl}/tests/${CONTEXT.testId}/reports/${CONTEXT.reportId}/stats`;
    const payload = JSON.stringify(Object.assign({
        runner_id: CONTEXT.runnerId,
        stats_time: Date.now().toString()
    }, body));

    let lastError;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        if (attempt > 1) {
            await wait(BACKOFF_BASE_MS * (attempt - 1));
        }
        try {
            const res = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'x-runner-id': CONTEXT.runnerId },
                body: payload
            });
            if (res.ok) {
                return;
            }
            lastError = new Error(`${res.status} - ${await res.text()}`);
            if (res.status < 500) {
                break;
            }
        } catch (error) {
            lastError = error;
        }
    }
    console.error(`predator plugin: failed to post ${body.phase_status}: ${lastError && lastError.message}`);
}

function Plugin(script, events) {
    // Artillery loads every plugin in the leader AND once per worker. Reporting
    // must happen exactly once, from the leader's aggregated events — the same
    // guard publish-metrics-style reporters use, inverted from expect's.
    if (typeof process.env.LOCAL_WORKER_ID !== 'undefined') {
        return this;
    }
    let firstIntermediate = true;
    const pending = [];

    events.on('phaseStarted', (info) => {
        pending.push(postStats({
            phase_index: info.index ? info.index.toString() : '0',
            phase_status: 'started_phase',
            data: JSON.stringify(info)
        }));
    });

    let lastStatsAt = Date.now();
    events.on('stats', (stats) => {
        const report = typeof stats.report === 'function' ? stats.report() : stats;
        delete report.latencies;
        // v2's legacy shim leaves rps.mean at 0; derive it from the window.
        const now = Date.now();
        const windowSeconds = Math.max(1, (now - lastStatsAt) / 1000);
        lastStatsAt = now;
        if (report.rps && !report.rps.mean) {
            report.rps.mean = Math.round((report.rps.count / windowSeconds) * 100) / 100;
        }
        pending.push(postStats({
            phase_status: firstIntermediate ? 'first_intermediate' : 'intermediate',
            data: JSON.stringify(report)
        }));
        firstIntermediate = false;
    });

    events.on('done', (report) => {
        pending.push(postStats({
            phase_status: 'done',
            data: JSON.stringify({ message: 'Test Finished' })
        }));
    });

    // artillery awaits cleanup before the process exits — the only reliable
    // place to flush in-flight posts (async 'done' handlers are not awaited).
    this.cleanup = async (done) => {
        await Promise.allSettled(pending);
        done();
    };

    return this;
}

module.exports = { Plugin };
