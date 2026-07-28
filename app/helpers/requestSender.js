const logger = require('../utils/logger');

// All runner -> predator API calls go through here: JSON in/out, with the same
// retry semantics requestxn provided (retry 5xx and network errors, 3 attempts,
// linear backoff). 4xx fails immediately — retrying a bad request won't fix it.
const MAX_ATTEMPTS = 3;
const BACKOFF_BASE_MS = 1000;

const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

module.exports.sendRequest = async (options) => {
    let lastError;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        if (attempt > 1) {
            await wait(BACKOFF_BASE_MS * (attempt - 1));
        }
        let response;
        try {
            response = await fetch(options.url, {
                method: options.method,
                headers: Object.assign({ 'Content-Type': 'application/json' }, options.headers),
                body: options.body === undefined ? undefined : JSON.stringify(options.body)
            });
        } catch (error) {
            lastError = error;
            logger.error(`Request to ${options.url} failed on the ${attempt} attempt with error ${error.message}`);
            continue;
        }

        const text = await response.text();
        if (response.ok) {
            logger.info(`Request to ${options.url} succeeded with status code ${response.status}`);
            return text ? JSON.parse(text) : undefined;
        }

        lastError = new Error(`${response.status} - ${text}`);
        lastError.statusCode = response.status;
        logger.error(`Request to ${options.url} failed on the ${attempt} attempt with error ${lastError.message}`);
        if (response.status < 500) {
            break;
        }
    }
    throw lastError;
};
