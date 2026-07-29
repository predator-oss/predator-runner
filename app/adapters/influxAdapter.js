// artillery v2's publish-metrics plugin has no influx reporter; the old
// artillery 1.x influxdb plugin is incompatible with the v2 plugin API.
// ponytail: fails loudly rather than silently dropping metrics — revisit if
// influx export is still needed (openreport/OTLP are candidates).
module.exports.buildMetricsPlugin = () => {
    throw new Error('Influx metrics export is not supported with artillery v2 yet. Use the prometheus exporter, or open an issue at predator-oss/predator-runner.');
};
