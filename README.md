# Usage

### demo
java -jar kudu-exporter-1.0-SNAPSHOT.jar --url http://localhost:8050/metrics

 param | describe | required
 ------------- | ------------- | -------------
 url | crawl kudu url data | true
 interval | crawl time interval, default 1000ms | false
 port | http port for prometheus, default 9098 | false
 exclude-file | TODO, for exclude unuseful metrics | false