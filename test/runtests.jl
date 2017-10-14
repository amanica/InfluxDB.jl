include("../src/InfluxDB.jl")
using InfluxDB
using Base.Test

measurement="cpu"
field="temp"

function testHidePassword()
    io=IOBuffer()
    connection = InfluxDB.InfluxConnection("http://localhost:8086",
        "julia-test",
        printQueries=true, username="someusername", password="topsecret")
    query = InfluxDB.buildQuery(connection)
    query["q"] = "SHOW measurements"
    InfluxDB.printQuery(connection, query, "query", io)
    printedQuery=String(take!(io))
    @test contains(printedQuery,"http://localhost:8086/query?")
    @test contains(printedQuery, "u=someusername")
    @test contains(printedQuery, "q=SHOW measurements")
    @test contains(printedQuery, "db=julia-test")
    @test contains(printedQuery, "p=*****")
    @test ismatch(r".+\?.+&.+&.+&.+", printedQuery)
    true
end

function waitForData(connection::InfluxDB.InfluxConnection)
    count = nothing
    for i in 1:1
        count = InfluxDB.count(connection, measurement, field)
        count != 0 && return count
        println("data not written yet, sleep a while")
        sleep(1)
    end
    count
end

function testWrite(connection::InfluxDB.InfluxConnection)
    # given
    InfluxDB.dropMeasurement(connection, measurement)
    @test InfluxDB.count(connection, measurement, field) == 0

    # when
    InfluxDB.write(connection, measurement, Dict(field=>35))

    # then
    @test waitForData(connection) == 1

    timeseries = InfluxDB.queryAsTimeArray(connection, measurement)
    println(timeseries)
    @test length(timeseries) == 1
    # TODO: check value
    true
end

@testset "Influxdb tests" begin
    connection = InfluxDB.InfluxConnection("http://localhost:8086", "julia-test",
        printQueries=true)
    InfluxDB.create_db(connection)

    @test testHidePassword();
    @test testWrite(connection)

end
