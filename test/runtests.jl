include("../src/InfluxDB.jl")
using InfluxDB
using Base.Test

@testset "Influxdb tests" begin
    connection = InfluxDB.InfluxConnection("http://localhost:8086", "julia-test")
    measurement="cpu"
    field="temp"
    InfluxDB.create_db(connection)

    InfluxDB.dropMeasurement(connection, measurement)
    @test InfluxDB.count(connection, measurement, field) == 0
    InfluxDB.write(connection, measurement, Dict(field=>35))
    @test InfluxDB.count(connection, measurement, field) == 1

    timeseries = InfluxDB.queryAsTimeArray(connection, measurement)
    println(timeseries)
    @test length(timeseries) == 1
end
