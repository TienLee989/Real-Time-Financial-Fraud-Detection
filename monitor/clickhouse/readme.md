CREATE DATABASE IF NOT EXISTS sms_logs;

CREATE TABLE IF NOT EXISTS sms_message
(
    id UUID,
    sms_id TEXT,
    partner LowCardinality(String),
    telco LowCardinality(String),
    phone String,
    message String,
    content String,
    content_type String,
    is_duplicate Int16,
    brandname LowCardinality(String),
    sms_type LowCardinality(String),
    status LowCardinality(String),
    stage LowCardinality(String),
    kafka_topic LowCardinality(String),

    source_ip LowCardinality(String),
    username String,
    priority Int16,

    send_dt Nullable(DateTime),
    received_dt Nullable(DateTime),
    latency_ms UInt32,

    error_code LowCardinality(String),
    error_msg String,

    event_time DateTime DEFAULT now(),    -- mốc thời gian ghi event
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (id, event_time)
SETTINGS storage_policy = 'tiered_policy', index_granularity = 8192

ALTER TABLE sms_message
MODIFY TTL
  event_time + INTERVAL 90 DAY TO VOLUME 'warm',   -- 0–90 ngày: SSD
  event_time + INTERVAL 365 DAY TO VOLUME 'cold';  -- 90–365 ngày: HDD
---------------------------------
{
  "id": null,
  "title": "SMS System Dashboard",
  "tags": ["sms", "fastapi", "kafka", "redis"],
  "timezone": "browser",
  "schemaVersion": 38,
  "version": 1,
  "refresh": "10s",
  "panels": [
    {
      "type": "row",
      "title": "Outbound (/send)",
      "collapsed": false,
      "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0}
    },
    {
      "type": "timeseries",
      "title": "Send Requests Total",
      "targets": [
        {
          "expr": "sum by(topic) (increase(sms_sends_total[5m]))",
          "legendFormat": "{{topic}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 1}
    },
    {
      "type": "timeseries",
      "title": "Send Errors by Type",
      "targets": [
        {
          "expr": "sum by(topic,error_type) (increase(sms_sends_errors_total[5m]))",
          "legendFormat": "{{topic}} - {{error_type}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 1}
    },
    {
      "type": "timeseries",
      "title": "Send Latency (p95)",
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum by(le, topic) (rate(sms_send_duration_seconds_bucket[5m])))",
          "legendFormat": "{{topic}} p95"
        }
      ],
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 9}
    },

    {
      "type": "row",
      "title": "Inbound (/callback)",
      "collapsed": false,
      "gridPos": {"h": 1, "w": 24, "x": 0, "y": 17}
    },
    {
      "type": "timeseries",
      "title": "Callbacks Total",
      "targets": [
        {
          "expr": "sum by(topic) (increase(sms_callbacks_total[5m]))",
          "legendFormat": "{{topic}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 18}
    },
    {
      "type": "timeseries",
      "title": "Callback Errors by Type",
      "targets": [
        {
          "expr": "sum by(topic,error_type) (increase(sms_callback_errors_total[5m]))",
          "legendFormat": "{{topic}} - {{error_type}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 18}
    },
    {
      "type": "timeseries",
      "title": "Callback Latency (p95)",
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum by(le, topic) (rate(sms_callback_duration_seconds_bucket[5m])))",
          "legendFormat": "{{topic}} p95"
        }
      ],
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 26}
    },

    {
      "type": "row",
      "title": "Kafka Producer",
      "collapsed": false,
      "gridPos": {"h": 1, "w": 24, "x": 0, "y": 34}
    },
    {
      "type": "timeseries",
      "title": "Kafka Published Messages",
      "targets": [
        {
          "expr": "sum by(topic) (increase(sms_published_total[5m]))",
          "legendFormat": "{{topic}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 35}
    },
    {
      "type": "timeseries",
      "title": "Kafka Publish Errors",
      "targets": [
        {
          "expr": "sum by(topic,error_type) (increase(sms_publish_errors_total[5m]))",
          "legendFormat": "{{topic}} - {{error_type}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 35}
    },
    {
      "type": "timeseries",
      "title": "Kafka Publish Latency (p95)",
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum by(le, topic) (rate(sms_publish_duration_seconds_bucket[5m])))",
          "legendFormat": "{{topic}} p95"
        }
      ],
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 43}
    },

    {
      "type": "row",
      "title": "Kafka Consumer",
      "collapsed": false,
      "gridPos": {"h": 1, "w": 24, "x": 0, "y": 51}
    },
    {
      "type": "timeseries",
      "title": "Consumer Messages",
      "targets": [
        {
          "expr": "sum by(topic) (increase(sms_consumer_messages_total[5m]))",
          "legendFormat": "{{topic}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 52}
    },
    {
      "type": "timeseries",
      "title": "Consumer Errors",
      "targets": [
        {
          "expr": "sum by(topic,error_type) (increase(sms_consumer_errors_total[5m]))",
          "legendFormat": "{{topic}} - {{error_type}}"
        }
      ],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 52}
    },
    {
      "type": "timeseries",
      "title": "Consumer Latency (p95)",
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum by(le, topic) (rate(sms_consumer_duration_seconds_bucket[5m])))",
          "legendFormat": "{{topic}} p95"
        }
      ],
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 60}
    },
    {
      "type": "timeseries",
      "title": "DLQ Messages",
      "targets": [
        {
          "expr": "sum by(topic) (increase(sms_dlq_total[5m]))",
          "legendFormat": "{{topic}}"
        }
      ],
      "gridPos": {"h": 6, "w": 24, "x": 0, "y": 68}
    },

    {
      "type": "row",
      "title": "Redis Queue",
      "collapsed": false,
      "gridPos": {"h": 1, "w": 24, "x": 0, "y": 74}
    },
    {
      "type": "stat",
      "title": "Pending Messages in Redis",
      "targets": [
        {
          "expr": "sms_pending_messages",
          "legendFormat": "{{topic}}"
        }
      ],
      "gridPos": {"h": 6, "w": 24, "x": 0, "y": 75}
    }
  ],
  "templating": {
    "list": []
  }
}
