package org.yewc.prometheus;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.yewc.prometheus.util.HttpReqUtil;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class SimpleTest {

    public static final CollectorRegistry registry = new CollectorRegistry();

    public static HTTPServer httpServer;

    public static Map<String, Gauge> gaugeMap = new HashMap<>();

    public void processRequest() throws Exception {
        String text = HttpReqUtil.get("http://10.17.4.11:8050/metrics");
        JSONArray result = JSON.parseArray(text);

        JSONObject data;
        JSONObject metricData;
        JSONArray metrics;
        Gauge tempGauge = null;
        String id, type, key, name;
        double value;
        for (int i = 0; i < result.size(); i++) {
            data = result.getJSONObject(i);
            id = data.getString("id");
            type = data.getString("type");
            metrics = data.getJSONArray("metrics");
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
                                .labelNames("id", "type")
                                .register(registry);
                        gaugeMap.put(key, tempGauge);
                    }

                    gaugeMap.get(key).labels(id, type).set(value);
                } catch (Exception e) {
                    System.out.println(e.getLocalizedMessage() + ": " + key);
                }

            }
        }
    }

    public void start() throws Exception {
        final SimpleTest test = this;
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        test.processRequest();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        InetSocketAddress socket = new InetSocketAddress(9099);

        this.httpServer = new HTTPServer(socket, this.registry);
    }

    public static void main(String[] args) throws Exception {
        SimpleTest test = new SimpleTest();
        test.start();
    }
}
