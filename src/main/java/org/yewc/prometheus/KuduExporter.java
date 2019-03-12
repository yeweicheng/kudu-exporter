package org.yewc.prometheus;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yewc.prometheus.util.HttpReqUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KuduExporter {

    private final static Logger LOGGER = LoggerFactory.getLogger(KuduExporter.class);

    private final static CollectorRegistry registry = new CollectorRegistry();

    private static HTTPServer httpServer;

    private static Map<String, Gauge> gaugeMap = new HashMap<>();

    private final static String PREFIX = "kudu_";

    private final static String PARTITION_FORMAT = "(HASH \\()(.+)(\\) PARTITION )([\\d]+)([\\, ]*)|(RANGE \\()(.+)(\\) PARTITION )(.+)";

    @Option(name="--url", usage="crape kudu url", required = true)
    private String url;

    @Option(name="--port", usage="web port")
    private int port = 9098;

    @Option(name="--interval", usage="crape kudu interval millis time")
    private int interval = 1000;

    @Option(name="--reload", usage="reload inculde & exclude file interval millis time")
    private int reload = 10000;

    @Option(name="--exclude-file", usage="exclude metrics list, text file will be fine, one row one metric")
    private String excludeFilePath;

    @Option(name="--include-file", usage="include metrics list, text file will be fine, one row one metric, highest priority, only include metrics will be recode")
    private String includeFilePath;

    private final Set<String> excludeMetrics = new HashSet<>();
    private final Set<String> includeMetrics = new HashSet<>();

    private static volatile boolean PROCESS_LOCKED = true;
    private static volatile boolean RELOAD_LOCKED = false;

    private static File excludeFile;
    private static File includeFile;

    private static Long excludeFileModified;
    private static Long includeFileModified;

    private static int retry;

    private void start() throws Exception {
        LOGGER.info("use url: " + url);
        LOGGER.info("use port: " + port);
        LOGGER.info("use interval: " + interval);
        LOGGER.info("use exclude file: " + excludeFilePath);
        LOGGER.info("use include file: " + includeFilePath);

        if (includeFilePath != null && !"".equals(includeFilePath.trim())) {
            includeFile = new File(includeFilePath);
            includeFileModified = includeFile.lastModified();
            readMetricsFile(includeFile, includeMetrics);
            LOGGER.info("include metrics size: {}", includeMetrics.size());
        }

        if (includeMetrics.isEmpty() && excludeFilePath != null && !"".equals(excludeFilePath.trim())) {
            excludeFile = new File(excludeFilePath);
            excludeFileModified = excludeFile.lastModified();
            readMetricsFile(excludeFile, excludeMetrics);
            LOGGER.info("exclude metrics size: {}", excludeMetrics.size());
        }

        new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(interval);
                        while (RELOAD_LOCKED) {
                            Thread.sleep(10);
                        }
                        PROCESS_LOCKED = true;
                        processRequest();
                        PROCESS_LOCKED = false;

                        LOGGER.debug("metrics processed success!");
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                    System.exit(1);
                }
        }).start();

        if (includeFile != null || excludeFile != null) {
            new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(reload);
                        if ((includeFile != null && includeFileModified != includeFile.lastModified())
                                || (excludeFile != null && excludeFileModified != excludeFile.lastModified())) {
                            if (includeFile != null) {
                                includeFileModified = includeFile.lastModified();
                            }

                            if (excludeFile != null) {
                                excludeFileModified = excludeFile.lastModified();
                            }
                        } else {
                            continue;
                        }

                        while (PROCESS_LOCKED) {
                            Thread.sleep(10);
                        }
                        RELOAD_LOCKED = true;

                        if (includeFile != null) {
                            includeMetrics.clear();
                            readMetricsFile(includeFile, includeMetrics);
                        } else if (excludeFile != null) {
                            excludeMetrics.clear();
                            readMetricsFile(excludeFile, excludeMetrics);
                        }
                        RELOAD_LOCKED = false;

                        LOGGER.info("metrics file reloaded success!");
                        LOGGER.info("include metrics size: {}", includeMetrics.size());
                        LOGGER.info("exclude metrics size: {}", excludeMetrics.size());
                    } catch (Exception e) {
                        RELOAD_LOCKED = false;
                        LOGGER.error("", e);
                    }
                }
            }).start();
        }

        InetSocketAddress socket = new InetSocketAddress(this.port);

        this.httpServer = new HTTPServer(socket, this.registry);
    }

    private void readMetricsFile(File file, Set<String> metricsSet) throws Exception {
        if (!file.exists()) {
            throw new RuntimeException("metrics file is not exists");
        }

        FileReader reader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String tempString = null;
        while ((tempString = bufferedReader.readLine()) != null) {
            metricsSet.add(tempString);
        }
        bufferedReader.close();
        reader.close();
    }

    public void processRequest() throws Exception {
        String text = null;
        try {
            text = HttpReqUtil.get(url);
            retry = 0;
        } catch (Exception e) {
            retry++;
            if (retry > 60) {
                LOGGER.error("retry time over" + retry + ", stop program", e);
                throw e;
            }

            LOGGER.error("retry time " + retry, e);
            return;
        }

        gaugeMap.entrySet().stream().forEach(entry -> entry.getValue().clear());

        JSONArray result = JSON.parseArray(text);

        JSONObject data;
        JSONObject metricData;
        JSONObject attributes;
        JSONArray metrics;
        Gauge tempGauge = null;
        String id, type, key, name, orginName;
        String[] allKeys = null;
        Set<String> valueKeys = null;
        List<String> newLabelValues = null;
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
                orginName = metricData.getString("name");
                if ("state".equals(orginName)) {
                    continue;
                }

                name = PREFIX + orginName;
                key = name;

                if (!includeMetrics.isEmpty()) {
                    if (!includeMetrics.contains(orginName)) {
                        if (gaugeMap.containsKey(key)) {
                            registry.unregister(gaugeMap.get(key));
                            gaugeMap.remove(key);
                        }
                        continue;
                    }
                } else if (!excludeMetrics.isEmpty() && excludeMetrics.contains(orginName)) {
                    if (gaugeMap.containsKey(key)) {
                        registry.unregister(gaugeMap.get(key));
                        gaugeMap.remove(key);
                    }
                    continue;
                }

                try {

                    if (metricData.containsKey("value")) {
                        value = metricData.getDouble("value");
                        if (!gaugeMap.containsKey(key)) {
                            tempGauge = Gauge.build()
                                    .name(name)
                                    .help("kudu metrics: " + name)
                                    .labelNames(attrKeys)
                                    .register(registry);
                            gaugeMap.put(key, tempGauge);
                        }

                        gaugeMap.get(key).labels(labelValues.toArray(new String[0])).set(value);

                    } else {
                        valueKeys = metricData.keySet();
                        valueKeys.remove("name");

                        allKeys = new String[attrKeys.length + 1];
                        for (int k = 0; k < attrKeys.length; k++) {
                            allKeys[k] = attrKeys[k];
                        }
                        allKeys[allKeys.length - 1] = "value_type";

                        if (!gaugeMap.containsKey(key)) {
                            tempGauge = Gauge.build()
                                    .name(name)
                                    .help("kudu metrics: " + name)
                                    .labelNames(allKeys)
                                    .register(registry);
                            gaugeMap.put(key, tempGauge);
                        }

                        for (String valueKey : valueKeys) {
                            newLabelValues = new ArrayList<>(labelValues.size() + 1);
                            for (int k = 0; k < labelValues.size(); k++) {
                                newLabelValues.add(labelValues.get(k));
                            }
                            newLabelValues.add(valueKey);
                            gaugeMap.get(key)
                                    .labels(newLabelValues.toArray(new String[0]))
                                    .set(metricData.getDouble(valueKey));
                        }
                    }

                } catch (Exception e) {
                    LOGGER.error(data.toJSONString(), e);
                }

            }
        }
    }

    public List<String> getPartitionData(String data) throws Exception {
        List<String> labelValues = new ArrayList<>();
        try {
            Matcher matcher = Pattern.compile(PARTITION_FORMAT).matcher(data);
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
        } catch (Exception e) {
            throw e;
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
