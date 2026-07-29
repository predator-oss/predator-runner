const should = require('should');
const prometheusAdapter = require('../../../app/adapters/prometheusAdapter');

const jobConfig = {
    reportId: '0d9d772d-ce0e-4318-af18-d695561f1320',
    testName: 'MickeysTest',
    cluster: 'Dev'
};

describe('Prometheus adapter test', () => {
    it('maps predator config onto artillery v2 publish-metrics (pushgateway)', () => {
        const pluginConfiguration = prometheusAdapter.buildMetricsPlugin({ push_gateway_url: 'url' }, jobConfig);
        should(pluginConfiguration).eql({
            'publish-metrics': [{
                type: 'prometheus',
                pushgateway: 'url',
                tags: [
                    'testName:MickeysTest',
                    'testRunId:0d9d772d-ce0e-4318-af18-d695561f1320',
                    'cluster:Dev'
                ]
            }]
        });
    });

    it('merges custom labels into tags', () => {
        const pluginConfiguration = prometheusAdapter.buildMetricsPlugin({
            push_gateway_url: 'url',
            labels: { key1: 'value1' }
        }, jobConfig);
        pluginConfiguration['publish-metrics'][0].tags.should.containEql('key1:value1');
    });
});
