'use strict';
require('./utils/verifier').verifyEnvironmentVars();

const { v4: uuid } = require('uuid');

const runner = require('./models/runner'),
    logger = require('./utils/logger'),
    jobConfig = require('./config/jobConfig'),
    reporterConnector = require('./connectors/reporterConnector'),
    errorHandler = require('./handler/errorHandler'),
    { version: PREDATOR_RUNNER_VERSION } = require('../package.json'),
    { verifyPredatorVersion } = require('./utils/versionCheck');

// artillery v2 runs as a separate process with its own boot and teardown, so
// the kill-switch needs real headroom beyond the configured duration.
const RUNNER_TIMEOUT_GRACE_MS = 30000;

const getContainerId = () => {
    let containerId = uuid();
    if (process.env.MARATHON_APP_ID) {
        let marathonAppId = process.env.MARATHON_APP_ID.split('/');
        containerId = marathonAppId[marathonAppId.length - 1];
    }
    return containerId;
};

let start = async () => {
    if (jobConfig.delayRunnerMs > 0) {
        await timeout(jobConfig.delayRunnerMs);
    }
    jobConfig.containerId = getContainerId();
    try {
        logger.info({ runner_config: jobConfig }, 'Initialized test runner');

        process.on('SIGTERM', async function () {
            logger.warn('Test aborted');
            await reporterConnector.postStats(jobConfig, {
                phase_status: 'aborted',
                data: JSON.stringify(jobConfig)
            });
            process.exit(1);
        });
        process.on('SIGUSR1', async function() {
            logger.info('Runner exceeded test duration, sending DONE status and existing');
            await reporterConnector.postStats(jobConfig, {
                phase_status: 'done',
                data: JSON.stringify({ message: 'Test Finished' })
            });
            process.exit(1);
        });
        verifyPredatorVersion(PREDATOR_RUNNER_VERSION, jobConfig.predatorVersion, logger);
        setTimeout(function() {
            process.kill(process.pid, 'SIGUSR1');
        }, (jobConfig.duration * 1000) + (jobConfig.delayRunnerMs || 0) + RUNNER_TIMEOUT_GRACE_MS);
        await runner.runTest(jobConfig);
        logger.info('Finished running test successfully');
        process.exit(0);
    } catch (err) {
        await errorHandler.handleError(jobConfig, err);
        process.exit(1);
    }
};

function timeout(ms) {
    logger.info(`sleeping for ${ms} ms before starting runner`);
    return new Promise(resolve => setTimeout(resolve, ms));
}

start();