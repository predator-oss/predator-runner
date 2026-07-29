// Maps predator's prometheus metrics config onto artillery v2's bundled
// publish-metrics plugin (pushgateway reporter).
module.exports.buildMetricsPlugin = (metricsConfig, jobConfig) => {
    const labels = Object.assign({
        testName: jobConfig.testName,
        testRunId: jobConfig.reportId,
        cluster: jobConfig.cluster
    }, metricsConfig.labels);

    const tags = Object.entries(labels)
        .filter(([, value]) => value !== undefined && value !== null)
        .map(([name, value]) => `${name}:${value}`);

    return {
        'publish-metrics': [{
            type: 'prometheus',
            pushgateway: metricsConfig.push_gateway_url,
            tags
        }]
    };
};
