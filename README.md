# Usage

### demo
java -jar kudu-exporter-1.0-SNAPSHOT.jar --url http://localhost:8050/metrics

 param | describe | required
 ------------- | ------------- | -------------
 url | crawl kudu url data | true
 port | http port for prometheus, default 9098 | false
 interval | crawl time interval, default 1000ms | false
 reload | reload inculde & exclude file, default 10000ms | false
 exclude-file | for exclude unuseful metrics, one row one metric | false
 include-file | for exclude unuseful metrics, one row one metric(highest priority) | false