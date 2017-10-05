using InfluxDB
using Base.Test

server = InfluxDB.InfluxServer("http://localhost:8086")
InfluxDB.create_db(server, "stats")
