package org.yewc.prometheus;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yewc.prometheus.util.HttpReqUtil;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KuduExporter {

    private final static Logger LOGGER = LoggerFactory.getLogger(KuduExporter.class);

    public static final CollectorRegistry registry = new CollectorRegistry();

    public static HTTPServer httpServer;

    public static Map<String, Gauge> gaugeMap = new HashMap<>();

    @Option(name="--url", usage="crape kudu url", required = true)
    private String url;

    @Option(name="--interval", usage="crape kudu interval millis time")
    private int interval = 1000;

    @Option(name="--port", usage="web port")
    private int port = 9098;

    @Option(name="--partition-format", usage="partition format string")
    private String partitionFormat = "(HASH \\()(.+)(\\) PARTITION )([\\d]+)([\\, ]*)|(RANGE \\()([a-zA-z]+)(\\) PARTITION )(.+)";

    @Option(name="--exclude-file", usage="TODO: exclude metrics list, text file will be fine")
    private String excludeFile;

    private void start() throws Exception {
        LOGGER.info("use url: " + url);
        LOGGER.info("use port: " + port);
        LOGGER.info("use interval: " + interval);
        LOGGER.info("use exclude file: " + excludeFile);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(interval);
                        processRequest();
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                    System.exit(1);
                }
            }
        }).start();

        InetSocketAddress socket = new InetSocketAddress(this.port);

        this.httpServer = new HTTPServer(socket, this.registry);
    }

    public void processRequest() throws Exception {
        String text = HttpReqUtil.get(url);
        JSONArray result = JSON.parseArray(text);

        JSONObject data;
        JSONObject metricData;
        JSONObject attributes;
        JSONArray metrics;
        Gauge tempGauge = null;
        String id, type, key, name;
        double value;
        for (int i = 0; i < result.size(); i++) {
            data = result.getJSONObject(i);
            id = data.getString("id");
            type = data.getString("type");
            attributes = data.getJSONObject("attributes");
            metrics = data.getJSONArray("metrics");

            Set<String> labelNames = new LinkedHashSet<>();
            labelNames.add("id");
            labelNames.add("type");
            labelNames.addAll(attributes.keySet());
            if (attributes.containsKey("partition")) {
                labelNames.add("partition_hash_column");
                labelNames.add("partition_hash_index");
                labelNames.add("partition_range_column");
                labelNames.add("partition_range_value");
            }
            String[] attrKeys = labelNames.toArray(new String[0]);

            List<String> labelValues = new ArrayList<>();
            labelValues.add(id);
            labelValues.add(type);
            for (int k = 2; k < attrKeys.length; k++) {
                if (attributes.containsKey(attrKeys[k])) {
                    labelValues.add(attributes.getString(attrKeys[k]));
                }
            }
            if (attributes.containsKey("partition")) {
                labelValues.addAll(getPartitionData(attributes.getString("partition")));
            }

            for (int j = 0; j < metrics.size(); j++) {
                metricData = metrics.getJSONObject(j);
                name = metricData.getString("name");
                if ("state".equals(name)) {
                    continue;
                }

                key = name;

                try {
                    value = metricData.containsKey("value")?metricData.getDouble("value"):metricData.getDouble("mean");

                    if (!gaugeMap.containsKey(key)) {
                        tempGauge = Gauge.build()
                                .name(name)
                                .help("kudu metrics: " + name)
                                .labelNames(attrKeys)
                                .register(registry);
                        gaugeMap.put(key, tempGauge);
                    }

                    gaugeMap.get(key).labels(labelValues.toArray(new String[0])).set(value);
                } catch (Exception e) {
                    LOGGER.error(e.getLocalizedMessage() + ": " + data.toJSONString());
                }

            }
        }
    }

    public List<String> getPartitionData(String data) throws Exception {
        List<String> labelValues = new ArrayList<>();

        Matcher matcher = Pattern.compile(partitionFormat).matcher(data);
        while (matcher.find()) {
            String holeStr = matcher.group(0);
            if (holeStr.startsWith("HASH")) {
                labelValues.add(matcher.group(2));
                labelValues.add(matcher.group(4));
            } else {
                labelValues.add(matcher.group(7));
                labelValues.add(matcher.group(9).replaceAll("\"", ""));
            }
        }

        if (data.startsWith("RANGE")) {
            labelValues.add(labelValues.get(0));
            labelValues.add(labelValues.get(1));
            labelValues.set(0, "");
            labelValues.set(1, "");
        } else if (data.startsWith("HASH") && !data.contains("RANGE")) {
            labelValues.add("");
            labelValues.add("");
        }

        return labelValues;
    }

    public static void main(String[] args) throws Exception {
        try {
            KuduExporter kuduExporter = new KuduExporter();
            CmdLineParser parser = new CmdLineParser(kuduExporter);
            parser.parseArgument(args);

            kuduExporter.start();
        } catch (Exception e) {
            LOGGER.error("out of control exception.", e);
            System.exit(1);
        }

//        String partitionFormat = "(HASH \\()(.+)(\\) PARTITION )([\\d]+)([\\, ]*)|(RANGE \\()([a-zA-z]+)(\\) PARTITION )(.+)";
////        String str = "HASH (system_default_id,xzc) PARTITION 1, RANGE (dt) PARTITION \\\"2019-02-15\\\" <= VALUES < \\\"2019-02-16\\\"";
//        String str = "RANGE (dt) PARTITION \\\"2019-02-15 00\\\" <= VALUES < \\\"2019-02-16 00\\\"";
////        String str = "RANGE (etltime) PARTITION UNBOUNDED";
//        Matcher m1 = Pattern.compile(partitionFormat).matcher(str);
//        while (m1.find()) {
//            for (int i = 0; i <= m1.groupCount(); i++) {
//                System.out.println(i + ":   " + m1.group(i));
//            }
//            System.out.println();
//        }
    }
}
