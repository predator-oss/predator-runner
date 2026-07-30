'use strict';

const should = require('should');
const { verifyPredatorVersion } = require('../../../app/utils/versionCheck');

const recorder = () => {
    const calls = { warn: [], error: [] };
    return {
        calls,
        warn: (obj, msg) => calls.warn.push(msg),
        error: (obj, msg) => calls.error.push(msg)
    };
};

describe('verifyPredatorVersion', function () {
    it('runs when the versions match exactly', function () {
        const logger = recorder();
        verifyPredatorVersion('1.11.0', '1.11.0', logger);
        logger.calls.warn.should.have.length(0);
        logger.calls.error.should.have.length(0);
    });

    it('runs, with a warning, when only the minor differs', function () {
        // The case that used to kill every run mid-release: a server pod that
        // has not re-pulled yet is still fully compatible.
        const logger = recorder();
        verifyPredatorVersion('1.11.0', '1.10.1', logger);
        logger.calls.error.should.have.length(0);
        logger.calls.warn.should.have.length(1);
    });

    it('runs when only the patch differs', function () {
        const logger = recorder();
        verifyPredatorVersion('1.11.2', '1.11.0', logger);
        logger.calls.error.should.have.length(0);
        logger.calls.warn.should.have.length(0);
    });

    it('refuses when the major differs, where the stats contract can break', function () {
        const logger = recorder();
        should.throws(() => verifyPredatorVersion('2.0.0', '1.11.0', logger), /Bad Predator-Runner version/);
        logger.calls.error.should.have.length(1);
    });

    it('continues when the predator version is missing or unparseable', function () {
        const logger = recorder();
        verifyPredatorVersion('1.11.0', undefined, logger);
        verifyPredatorVersion('1.11.0', 'not-a-version', logger);
        logger.calls.error.should.have.length(0);
        logger.calls.warn.should.have.length(2);
    });
});
