include("../src/InfluxDB.jl")
using InfluxDB
using Base.Test
using Base.Dates
using TimeSeries

measurement="cpu"
field="temp"

TEST_INFLUX_URL="http://localhost:8086"
TEST_INFLUX_DB="julia-test"

function testHidePassword()
    io=IOBuffer()
    connection = InfluxDB.InfluxConnection(TEST_INFLUX_URL,
        TEST_INFLUX_DB,
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

function testWrite(connection::InfluxDB.InfluxConnection)
    # given
    InfluxDB.dropMeasurement(connection, measurement)
    @test InfluxDB.count(connection, measurement, field) == 0

    # when
    InfluxDB.write(connection, measurement, Dict(field=>35))

    # then
    @test InfluxDB.count(connection, measurement, field) == 1
    @test InfluxDB.showFieldKeys(connection;
        fromMeasurement=measurement)[measurement] == [field]

    true
end

function testReadTimeSeries(connection::InfluxDB.InfluxConnection)
    # given
    InfluxDB.dropMeasurement(connection, measurement)
    InfluxDB.write(connection, measurement, Dict(field=>35))
    @test InfluxDB.count(connection, measurement, field) == 1

    # when
    timeseries = InfluxDB.queryAsTimeArray(connection, measurement)

    # then
    println(timeseries)
    @test length(timeseries) == 1
    # TODO: check value

    InfluxDB.write(connection, measurement, Dict(field=>36),
        timestamp=Dates.datetime2unix(DateTime(2017, 2, 1, 0, 0))
    )

    timeseries = InfluxDB.queryAsTimeArray(connection, measurement,
        from=DateTime(2017, 1, 1, 0, 0), to=DateTime(2017, 2, 2, 0, 0))
    println(timeseries)
    @test length(timeseries) == 1

    true
end

function testWriteTimeArray(connection::InfluxDB.InfluxConnection)
    InfluxDB.dropMeasurement(connection, measurement)
    period=Second(5)
    from=DateTime(2017, 1, 1, 0, 0)
    to=DateTime(2017, 1, 1, 0, 1)
    dates=collect(from:period:to)
    colNames=["a.a", "b.b"]
    data=rand(length(dates), length(colNames))
    data[:,2].*=10
    @show timearray = TimeArray(dates, data, colNames)

    InfluxDB.write(connection, measurement, timearray)

    readTimeseries = InfluxDB.queryAsTimeArray(connection, measurement,
        from=DateTime(2017, 1, 1, 0, 0), to=DateTime(2017, 2, 2, 0, 0)
    )
    println(readTimeseries)
    @test length(readTimeseries) == length(timearray)

    true
end

function testSmartUrl()
    connection = InfluxDB.InfluxConnection("localhost:0", TEST_INFLUX_DB,
        printQueries=true)
    InfluxDB.create_db(connection)
    true
end

@testset "Influxdb tests" begin
    connection = InfluxDB.InfluxConnection(TEST_INFLUX_URL, TEST_INFLUX_DB,
        printQueries=true)
    InfluxDB.create_db(connection)

    @test testSmartUrl()
    @test testHidePassword()
    @test testWrite(connection)
    @test testReadTimeSeries(connection)
    @test testWriteTimeArray(connection)
end
