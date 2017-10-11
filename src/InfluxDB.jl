__precompile__()
module InfluxDB

export InfluxConnection, create_db, query, query_series,
    rawQuery, showMeasurements, showFieldKeys, count, write,
    queryAsTimeArray, dropMeasurement
import Base: write

using JSON
using DataFrames
using TimeSeries
using HTTP

# A connection that we will be communicating with
# TODO: add a debug flag / show queries flag
type InfluxConnection
    # HTTP API endpoints
    addr::HTTP.URI
    dbName::AbstractString

    # Optional authentication stuffage
    username::Union{Void,AbstractString}
    password::Union{Void,AbstractString}

    # Build a connection object that we can use in queries from now on
    function InfluxConnection(address::AbstractString, dbName::AbstractString;
        username::Union{Void,AbstractString}=nothing,
        password::Union{Void,AbstractString}=nothing)
        # If there wasn't a schema defined (we only recognize http/https), default to http
        if !ismatch(r"^https?://", address)
            uri = HTTP.URI("http://$address")
        else
            uri = HTTP.URI(address)
        end

        # If we didn't get an explicit port, default to 8086
        if HTTP.port(uri) == 0
            uri =  HTTP.URI(HTTP.scheme(uri), HTTP.host(uri), 8086, HTTP.path(uri))
        end

        # URIs are the new hotness
        return new(uri, dbName, username, password)
    end
end

# Returns a Dict that includes the db and authentication if needed
function buildQuery(connection::InfluxConnection)
    query = Dict("db"=>connection.dbName)
    if connection.username != nothing && connection.password != nothing
        query["u"] = connection.username.value
        query["p"] = connection.password.value
    end
    query
end

function checkResponse(response::HTTP.Response, expectedStatus=200)
    code = HTTP.status(response)
    if code != expectedStatus
        @show response
        error(HTTP.statustext(response) * ":\n" * HTTP.body(response))
    end
end

function rawQuery(connection::InfluxConnection, query::Dict)
    response = HTTP.get("$(connection.addr)/query"; query=query)
    checkResponse(response)
    JSON.parse(HTTP.body(response))
end

# Returns a list of the measuresments
function showMeasurements(connection::InfluxConnection)
    query = buildQuery(connection)
    query["q"] = "SHOW measurements"
    results = rawQuery(connection, query)["results"][1]
    if length(results) == 0
        return []
    end
    results["series"][1]["values"][1]
end

function dropMeasurement(connection::InfluxConnection,
        measurement::AbstractString)
    query = buildQuery(connection)
    query["q"] = "DROP MEASUREMENT \"$measurement\""
    rawQuery(connection, query)
end


# Returns a dictionary mesurement=>fieldList
function showFieldKeys(connection::InfluxConnection;
        fromMeasurement::Union{Void,AbstractString}=nothing)
    fromClause=fromMeasurement==nothing?"":" FROM \"$fromMeasurement\""
    query = buildQuery(connection)
    query["q"] = "SHOW FIELD KEYS$fromClause"
    results = rawQuery(connection, query)["results"][1]
    ret = Dict()
    if length(results) == 0
        return ret
    end
    for series in results["series"]
        name = series["name"]
        values = series["values"][1]
        ret[name] = values
    end
    ret
end

# Count the number of values for a measurment field
function count(connection::InfluxConnection,
        measurement::AbstractString, fieldKey::AbstractString)
    query = buildQuery(connection)
    @show query["q"] = "SELECT count(\"$fieldKey\") FROM \"$measurement\""
    # {"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}
    results = rawQuery(connection, query)["results"][1]
    if length(results) == 0
        return 0
    end
    series_dict = results["series"][1]
    series_dict["values"][1][2]
end

#=
{"results":[{"series":[{"name":"cpu","columns":["time","temp"],
"values":[["2017-10-03T22:00:58Z",35],["2017-10-03T22:02:20Z",35]]}]}]}

{"results":[{"series":[{"name":"temperature",
"columns":["time","external","internal","machine","type"],
"values":[["2017-10-07T22:08:27.097028615Z",25,37,"unit42","assembly"]]}]}]}
=#
# TODO: support specifying a date range
function queryAsTimeArray(connection::InfluxConnection,
        measurement::AbstractString; chunk_size::Integer=10000)
    query = buildQuery(connection)
    query["q"] = "SELECT * FROM \"$measurement\""
    # Grab result, turn it into a TimeArray
    series_dict = rawQuery(connection, query)["results"][1]["series"][1]
    columnCount=length(series_dict["columns"])
    valueCount=length(series_dict["values"])
    # TODO: support other date and value types
    dates=Vector{DateTime}(valueCount)
    values=Matrix{Float64}(valueCount,columnCount-1)
    for (i, row) in enumerate(series_dict["values"])
        # TODO: support millisecond precision eg. "2017-10-07T22:08:27.097028615Z"
        dates[i] = parse(DateTime, row[1],  @dateformat_str "yyyy-mm-dd\\THH:MM:SSZ")
        values[i,:] = Vector{Float64}(row[2:end])
    end
    TimeArray(dates, values, Vector{String}(series_dict["columns"][2:end]))
end

# Grab a timeseries as a dataframe
# deprecated, is a timeseries not better than a dataframe for timeseries data?
function query_series(connection::InfluxConnection,
        measurement::AbstractString; chunk_size::Integer=10000)
    query = buildQuery(connection)
    query["q"] = "SELECT * FROM \"$measurement\""
    # Grab result, turn it into a dataframe
    series_dict = rawQuery(connection, query)["results"][1]["series"][1]
    df = DataFrame()
    for (measurement_idx, column) in enumerate(series_dict["columns"])
       df[Symbol(column[measurement_idx])] = [x[measurement_idx] for x in series_dict["values"]]
    end
    return df
end

# Create a database! (if needed)
function create_db(connection::InfluxConnection)
    query = buildQuery(connection)
    #maybe need to unset the db..
    query["q"] = "CREATE DATABASE \"$(connection.dbName)\""
    rawQuery(connection, query)
end

function write(connection::InfluxConnection, measurement::AbstractString, values::Dict;
                            tags=Dict{AbstractString,AbstractString}(), timestamp::Float64=time())
    if isempty(values)
        throw(ArgumentError("Must provide at least one value!"))
    end

    # Start by building our query dict, pointing at a particular database and timestamp precision
    query = buildQuery(connection)
    query["precision"]="s"

    # Next, string of tags, if we have any
    tagstring = join([",$key=$val" for (key, val) in tags])

    # Next, our values
    valuestring = join(["$key=$val" for (key, val) in values], ",")

    # Finally, convert timestamp to seconds
    timestring = "$(round(Int64,timestamp))"

    # Put them all together to get a data string
    datastr = "$(measurement)$(tagstring) $(valuestring) $(timestring)"

    # Authenticate ourselves, if we need to
    response = HTTP.post("$(connection.addr)/write"; query=query, body=datastr)
    checkResponse(response, 204)
end

end # module
