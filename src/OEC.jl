module OEC

using Arrow
using CSV
using DataFrames
using Dates
using FromDigits
using Glob
using HTTP
using Logging
using Mmap: mmap
using URIs
using ZipArchives: ZipReader, zip_names, zip_readentry, zip_openentry

export unzip_sv, date_int_to_date, list_sv_zip_files, transform_sv, write_arrow

const scratchdir = "/scratch/$(get(ENV, "USER", nothing))"

const BOL_data_scratchdir = joinpath(scratchdir, "temp/bi_dpi/data/OEC/bulk/Bill_of_Lading")

include("constants.jl")

function set_type(i, name)
    if occursin(date_regex, string(name))
        return String
    end
    nothing
end

function set_pooled_cols(i, name)
    if Symbol(name) in categoricals
        return true
    end
    false
end

"""
# Introduction

Convert a date encoded like the following integer
to a proper Date object (which can be recognized to
be encoded as, say, the proper Arrow date encoding).
E.g. 20240109 -> Date(2024, 1, 9)

# Arguments

- `i`: the integer to convert
"""
function date_int_to_date(i::Integer)
    d, fd = digits(i), fromdigits
    @assert length(d) == 8
    Date(fd(d[5:8]), fd(d[3:4]), fd(d[1:2]))
end

"""
# Introduction

Extract the first file from a zip archive,
assuming it's a comma or tab separated
value (sv) file.

# Arguments

- `path`: path to the zipped tab or comma separated file
"""
function unzip_sv(path::AbstractString)
    archive = ZipReader(mmap(open(path)))
    files = zip_names(archive)
    @info "files in archive:" files
    zip_readentry(archive, files[1])
end

function read_sv(svfile)
    CSV.File(svfile; stringtype=String, pool=false, types=set_type)
end

function list_sv_zip_files(dirpath::AbstractString)
    glob("*.[t,c]sv.zip", dirpath)
end

function transform_sv(csv::CSV.File)
    df = DataFrame(csv)
    c = intersect(date_int_cols, propertynames(df))
    transform!(df, c .=> (x -> date_int_to_date.(x)) .=> c)
    c = intersect((:year, :month, :day), propertynames(df))
    if length(c) == 3
        df = transform!(df, c => ByRow((y, m, d) -> Date(y, m, d)) => :date)[!, Not(c)]
    end
    df
end

function write_arrow(
    input::Union{Vector{<:AbstractString},AbstractString,Nothing},
    outdir::AbstractString="./",
    overwriteexisting::Bool=false,
)
    if input === nothing
        return nothing
    end
    mkpath(outdir)
    logfile_io = open(joinpath(outdir, "write_arrow_$(string(today())).log"), "a+")
    global_logger(SimpleLogger(logfile_io))
    if typeof(input) <: AbstractString
        input = [input]
    end
    for p in input
        outpath = joinpath(outdir, splitext(splitext(basename(p))[1])[1]) * ".arrow"
        if overwriteexisting || !isfile(outpath)
            @info "attempting to write " outpath
            try
                Arrow.write(outpath, read_sv(unzip_sv(p)); file=true)
            catch e
                @warn e
            end
            flush(logfile_io)
            Base.GC.gc()
        end
    end
    close(logfile_io)
end

#eachindex
#@inbounds

function clean_nodelist(
    df::AbstractDataFrame,
    keycol::Symbol,
    labelcol::Symbol,
    metacol::Union{Symbol,Nothing}=nothing,
    weightcol::Union{Symbol,Nothing}=nothing,
)
    uniquecols = [keycol, labelcol]
    cols = uniquecols
    if !isnothing(metacol)
        push!(cols, metacol)
    end
    if !isnothing(weightcol)
        push!(cols, weightcol)
    end
    df = df[!, cols]
    @view df[completecases(df, uniquecols).&.!nonunique(df, uniquecols), :]
end

function join_nodelists(
    df_left::AbstractDataFrame,
    df_right::AbstractDataFrame,
)
    innerjoin(df_left, df_right, on=names(df_left)[1], makeunique=true)
end

function get_api_key()
    env_var_name = "OEC_API_KEY"
    key = get(ENV, env_var_name, nothing)
    if isnothing(key)
        @warn "No API set in the $env_var_name environment variable. Please enter your API key."
        key = readline()
        ENV[env_var_name] = key
    end
    key
end

function get_uri(path::AbstractString="/api/dload-proxy"; filepath::AbstractString="")
    @assert !isempty(path)
    @assert !isempty(filepath)
    apikey = get_api_key()
    path = joinpath("/", path)
    filepath = escapeuri(filepath)
    uri = URI(; scheme="https", host=domain, path=path, query="token=$apikey&file=$filepath")
    uri
end

function get_bulk_BOL_import(
    start_year_month="2021-01", end_year_month=Dates.format(today(), "yyyy-mm");
    outdir::AbstractString="./data/OEC/bulk",
    overwriteexisting::Bool=false,
)
    # USA_Exports_2021_01.csv.zip
    serverdir = "bill_of_lading/BOL USA/Imports"
    outdir = joinpath(outdir, serverdir)
    mkpath(outdir)
    @debug abspath(outdir)
    date_month_range = Date(start_year_month):Month(1):Date(end_year_month)
    date_month_range = [Dates.format(ym, "yyyy_mm") for ym in date_month_range]

    for year_month in date_month_range
        year = split(year_month, "_")[1]
        delim = year < "2024" ? "tsv" : "csv"
        filename = "USA_Imports_$year_month.$delim.zip"
        filepath = joinpath(serverdir, filename)
        @debug filepath
        outpath = joinpath(outdir, filename)
        if isfile(outpath) && !overwriteexisting
            @info "Skipping $filepath."
            continue
        end
        uri = get_uri(filepath=filepath)
        @debug uri
        response = HTTP.get(uri)
        if response.status == 200
            open(outpath, "w") do f
                write(f, response.body)
            end

        else
            @warn "Failed to download $filepath"
        end
    end

end

end # module OEC
