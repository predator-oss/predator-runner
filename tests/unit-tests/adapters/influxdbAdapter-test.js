const should = require('should');
const influxAdapter = require('../../../app/adapters/influxAdapter');

describe('Influxdb adapter test', () => {
    it('fails loudly: influx export is not supported with artillery v2', () => {
        should(() => influxAdapter.buildMetricsPlugin({}, {})).throw(/not supported with artillery v2/);
    });
});
