__precompile__()
module InfluxDB

export InfluxConnection, create_db, query, query_series, rawQuery, showMeasurements, showFieldKeys, count, write
import Base: write

using JSON
using DataFrames
#using Compat
using HTTP

# A connection that we will be communicating with
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

# Grab a timeseries
function query_series(connection::InfluxConnection,
        measurement::AbstractString; chunk_size::Integer=10000)
    query = buildQuery(connection)
    query["q"] = "SELECT * FROM \"$measurement\""
    # Grab result, turn it into a dataframe
    series_dict = rawQuery(connection, query)["results"][1]["series"][1]
    df = DataFrame()
    #XXX: could use enumerate to save a lookup..
    for measurement_idx in 1:length(series_dict["columns"])
       df[Symbol(series_dict["columns"][measurement_idx])] = [x[measurement_idx] for x in series_dict["values"]]
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
