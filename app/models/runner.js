'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { parse: csv } = require('csv-parse/sync'),
    testFileConnector = require('../connectors/testFileConnector'),
    fileDownloadConnector = require('../connectors/fileDownloadConnector'),
    reporterConnector = require('../connectors/reporterConnector'),
    logger = require('../utils/logger'),
    progressCalculator = require('../helpers/progressCalculator'),
    constants = require('../utils/consts');

// Artillery v2 runs as its own process (its package exports forbid embedding),
// with artillery-plugin-predator inside it translating stats back to predator.
// This module prepares the script on disk and supervises the artillery run.

module.exports.runTest = async (jobConfig) => {
    const test = await testFileConnector.getTest(jobConfig);
    const processorJavascript = await getProcessorJavascript(jobConfig, test);
    const csvData = await getCSVData(jobConfig, test);
    await reporterConnector.subscribeToReport(jobConfig, test);

    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'predator-run-'));
    updateTestParameters(jobConfig, test.artillery_test, processorJavascript, csvData, workDir);
    logger.info(`Starting test: ${test.name}, testId: ${test.id}`);
    progressCalculator.calculateTotalNumberOfScenarios(jobConfig);

    const scriptPath = path.join(workDir, 'script.json');
    fs.writeFileSync(scriptPath, JSON.stringify(test.artillery_test));

    // artillery's exports map blocks require.resolve into the package, so walk
    // the module paths for the real bin file the .bin shim points at.
    const artilleryBin = module.paths
        .map(p => path.join(p, 'artillery', 'bin', 'run'))
        .find(p => fs.existsSync(p));
    if (!artilleryBin) {
        throw new Error('artillery binary not found in module paths');
    }
    logger.info({ script: test.artillery_test.config }, 'Spawning artillery');

    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [artilleryBin, 'run', scriptPath], {
            stdio: ['ignore', 'inherit', 'inherit'],
            env: Object.assign({}, process.env, {
                PREDATOR_URL: jobConfig.predatorUrl,
                TEST_ID: jobConfig.testId,
                REPORT_ID: jobConfig.reportId,
                RUNNER_ID: jobConfig.containerId,
                ARTILLERY_DISABLE_TELEMETRY: 'true'
            })
        });
        child.on('error', reject);
        child.on('exit', (code) => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error(`artillery exited with code ${code}`));
            }
        });
    });
};

function updateRunningParameters(testFile, jobConfig) {
    testFile.config.phases[0].duration = jobConfig.duration;

    if (jobConfig.jobType === constants.FUNCTIONAL_TEST) {
        testFile.config.phases[0].arrivalCount = jobConfig.arrivalCount;
        delete testFile.config.phases[0].arrivalRate;
        delete testFile.config.phases[0].rampTo;
    } else {
        testFile.config.phases[0].arrivalRate = jobConfig.arrivalRate;
        if (!jobConfig.rampTo) {
            delete testFile.config.phases[0].rampTo;
        } else {
            testFile.config.phases[0].rampTo = jobConfig.rampTo;
        }
    }

    if (!jobConfig.maxVusers) {
        delete testFile.config.phases[0].maxVusers;
    } else {
        testFile.config.phases[0].maxVusers = jobConfig.maxVusers;
    }
}

const updateTestParameters = (jobConfig, testFile, processorJavascript, csvData, workDir) => {
    if (!testFile.config.plugins) {
        testFile.config.plugins = {};
    }

    // Always report back to predator.
    testFile.config.plugins.predator = {};

    if (isTestHasExpectations(testFile)) {
        testFile.config.plugins.expect = { reportFailuresAsErrors: true };
    } else {
        delete testFile.config.plugins.expect;
    }

    if (jobConfig.metricsExportConfig && jobConfig.metricsPluginName) {
        injectMetricsPlugins(testFile, jobConfig);
    }

    if (processorJavascript) {
        // v2 loads processors from a file path relative to the script.
        fs.writeFileSync(path.join(workDir, 'processor.js'), processorJavascript);
        testFile.config.processor = './processor.js';
    }

    if (csvData) {
        fs.writeFileSync(path.join(workDir, 'payload.csv'), csvData.raw);
        testFile.config.payload = { path: './payload.csv', fields: csvData.fields, skipHeader: true };
    }

    if (!testFile.config.phases) {
        testFile.config.phases = [{}];
    }
    if (!testFile.config.http) {
        testFile.config.http = {};
    }
    testFile.config.http.pool = jobConfig.httpPoolSize;

    updateRunningParameters(testFile, jobConfig);
    logger.info({ updated_test_config: testFile.config }, 'Test successfully updated parameters');
};

function injectMetricsPlugins(testFile, jobConfig) {
    // Both adapters now target artillery's official publish-metrics plugin.
    const metricsPluginName = jobConfig.metricsPluginName.toLowerCase();
    const metricsAdapter = require(`../adapters/${metricsPluginName}Adapter`);
    const asciiMetricsExportConfig = (Buffer.from(jobConfig.metricsExportConfig, 'base64').toString('ascii'));
    const parsedMetricsConfig = JSON.parse(asciiMetricsExportConfig);

    const metricsPlugin = metricsAdapter.buildMetricsPlugin(parsedMetricsConfig, jobConfig);
    Object.assign(testFile.config.plugins, metricsPlugin);
}

async function getProcessorJavascript(jobConfig, test) {
    let javascript;
    if (test.file_id) {
        logger.warn('DEPRECATED: Using file_id in tests is deprecated. Please use the Processors API.');
        const fileContentBase64 = await fileDownloadConnector.getFile(jobConfig, test.file_id);
        javascript = Buffer.from(fileContentBase64, 'base64').toString('utf8');
    } else if (test.processor_id) {
        const processor = await fileDownloadConnector.getProcessor(jobConfig, test.processor_id);
        javascript = processor.javascript;
    }
    return javascript;
}

async function getCSVData(jobConfig, test) {
    const csvFileId = test.csv_file_id;
    if (!csvFileId) {
        return;
    }
    const payload = await fileDownloadConnector.getFile(jobConfig, csvFileId);
    let csvData;
    try {
        csvData = csv(payload);
    } catch (error) {
        throw new Error(`Failure to parse csv file with id: ${csvFileId}\n${error}`);
    }
    const fields = csvData.shift();

    logger.info({
        csv_file_id: csvFileId,
        headers: fields,
        number_of_rows: csvData.length,
        first_row: csvData[0]
    }, 'Parsed CSV successfully');
    return { fields, data: csvData, raw: payload };
}

function isTestHasExpectations(testFile) {
    let hasExpectations = false;
    testFile.scenarios.forEach((scenario) => {
        scenario.flow.forEach((request) => {
            const method = Object.keys(request)[0];
            if (request[method].expect && request[method].expect.length > 0) {
                hasExpectations = true;
            }
        });
    });
    return hasExpectations;
}
