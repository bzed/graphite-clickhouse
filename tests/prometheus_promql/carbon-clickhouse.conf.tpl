[[carbon]]
  protocol = "tcp"
  address = "0.0.0.0:2003"

[[prometheus]]
  enabled = true
  address = "0.0.0.0:9092"
  path = "/metrics"

[clickhouse]
  address = "{{.CLICKHOUSE_URL}}"
  database = "graphite"
  
  [[clickhouse.table]]
    name = "graphite"
    engine = "GraphiteMergeTree()"
    
    [clickhouse.table.structure]
      date = "Date"
      name = "Path"
      value = "Value"
      timestamp = "Timestamp"
      version = "Version"
    
    [clickhouse.table.archive]
      pattern = "{date}/{name}"
      function = "avg"

[graphite]
  prefix = "carbon.agents.{hostname}."
  interval = 60
  pattern = "{prefix}{metric}"

[logging]
  level = "info"
  file = "/var/log/carbon-clickhouse/carbon-clickhouse.log"