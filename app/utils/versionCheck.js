'use strict';

const semver = require('semver');

// The runner and the server talk over a small, stable contract: fetch the test,
// subscribe, post stats. That contract only changes on a major version, so only
// a major mismatch is a real incompatibility.
//
// Refusing to start on a *minor* difference made every release a two-step
// dance: a server pod that had not re-pulled yet would reject perfectly
// compatible runners, and the run died before generating any load — reporting
// nothing but a failed job with zero subscribers, which reads as "the test
// broke" rather than "the images are a version apart". A minor or patch
// difference is now a warning and the run proceeds.
module.exports.verifyPredatorVersion = function (runnerVersion, predatorVersion, logger) {
    const predatorSemver = semver.coerce(predatorVersion);

    if (!predatorSemver) {
        logger.warn({ predator_runner_version: runnerVersion, predator_version: predatorVersion },
            'Could not parse Predator version, continuing');
        return;
    }

    if (semver.major(runnerVersion) !== semver.major(predatorSemver)) {
        logger.error({ predator_runner_version: runnerVersion, predator_version: predatorVersion },
            'Predator Runner and Predator major versions differ, the stats contract is incompatible');
        throw new Error('Bad Predator-Runner version');
    }

    if (semver.minor(runnerVersion) !== semver.minor(predatorSemver)) {
        logger.warn({ predator_runner_version: runnerVersion, predator_version: predatorVersion },
            'Predator Runner and Predator minor versions differ, running anyway');
    }
};
