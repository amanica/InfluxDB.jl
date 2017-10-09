using InfluxDB
using Base.Test

@testset "Influxdb tests" begin
    connection = InfluxDB.InfluxConnection("http://localhost:8086", "julia-test")
    InfluxDB.create_db(connection)
    @test InfluxDB.count(connection, "aMeasurement", "aField") == 0
    InfluxDB.write(connection, "cpu", Dict("temp"=>35))
end
