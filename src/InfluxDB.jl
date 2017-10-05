__precompile__()
module InfluxDB

export InfluxServer, create_db, query
import Base: write

using JSON
#using Requests
using DataFrames
using Compat
using HTTP

# A server that we will be communicating with
type InfluxServer
    # HTTP API endpoints
    addr::HTTP.URI

    # Optional authentication stuffage
    username::Nullable{AbstractString}
    password::Nullable{AbstractString}

    # Build a server object that we can use in queries from now on
    function InfluxServer(address::AbstractString; username=Nullable{AbstractString}(), password=Nullable{AbstractString}())
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

        if !isa(username, Nullable)
            username = Nullable(username)
        end
        if !isa(password, Nullable)
            password = Nulllable(password)
        end
        @show uri
        # URIs are the new hotness
        return new(uri, username, password)
    end
end

# Add authentication to a query dict, if we need to
function authenticate!(server::InfluxServer, query::Dict)
    if !isnull(server.username) && !isnull(server.password)
        query["u"] = server.username.value
        query["p"] = server.password.value
    end
end


# Grab a timeseries
function query_series( server::InfluxServer, db::AbstractString, name::AbstractString;
                       chunk_size::Integer=10000)
    query = Dict("db"=>db, "q"=>"SELECT * from $name")

    authenticate!(server, query)
    @show url="$(server.addr)/query"
    response = HTTP.get(url; query=query)
    if response.status != 200
        error(HTTP.body(response))
    end

    # Grab result, turn it into a dataframe
    series_dict = JSON.parse(HTTP.body(response))["results"][1]["series"][1]
    df = DataFrame()
    for name_idx in 1:length(series_dict["columns"])
       df[Symbol(series_dict["columns"][name_idx])] = [x[name_idx] for x in series_dict["values"]]
    end
    return df
end

# Create a database!
function create_db(server::InfluxServer, db::AbstractString)
    query = Dict("q"=>"CREATE DATABASE \"$db\"")

    authenticate!(server, query)
    response = get("$(server.addr)query"; query=query)
    if response.status != 200
        error(String(response.data))
    end
end

function write( server::InfluxServer, db::AbstractString, name::AbstractString, values::Dict;
                            tags=Dict{AbstractString,AbstractString}(), timestamp::Float64=time())
    if isempty(values)
        throw(ArgumentError("Must provide at least one value!"))
    end

    # Start by building our query dict, pointing at a particular database and timestamp precision
    query = Dict("db"=>db, "precision"=>"s")

    # Next, string of tags, if we have any
    tagstring = join([",$key=$val" for (key, val) in tags])

    # Next, our values
    valuestring = join(["$key=$val" for (key, val) in values], ",")

    # Finally, convert timestamp to seconds
    timestring = "$(round(Int64,timestamp))"

    # Put them all together to get a data string
    datastr = "$(name)$(tagstring) $(valuestring) $(timestring)"

    # Authenticate ourselves, if we need to
    authenticate!(server, query)
    response = post("$(server.addr)write"; query=query, data=datastr)
    if response.status != 204
        error(String(response.data))
    end
end

end # module
