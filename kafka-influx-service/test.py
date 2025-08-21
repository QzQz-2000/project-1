from influxdb_client import InfluxDBClient

client = InfluxDBClient(url="http://localhost:8086", token="mytoken", org="myorg")

query = '''
from(bucket: "mybucket")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "device_telemetry")
  |> count()
'''

tables = client.query_api().query(query)
for table in tables:
    for record in table.records:
        print(record)
