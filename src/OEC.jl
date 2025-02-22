module OEC

using Arrow
using CSV
using DataFrames
using Dates
using FromDigits
using Glob
using Logging
using Mmap: mmap
using ZipArchives: ZipReader, zip_names, zip_readentry, zip_openentry

export unzip_sv, date_int_to_date, list_sv_zip_files, transform_sv, write_arrow

const scratchdir = "/scratch/$(get(ENV, "USER", nothing))"

const BOL_data_scratchdir = joinpath(scratchdir, "temp/bi_dpi/data/OEC/bulk/Bill_of_Lading")

include("constants.jl")

function set_type(i, name)
    if occursin(date_regex, string(name)) return String end
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
    if !isnothing(metacol) push!(cols, metacol) end
    if !isnothing(weightcol) push!(cols, weightcol) end
    df = df[!, cols]
    @view df[completecases(df, uniquecols) .& .!nonunique(df, uniquecols), :]
end

end # module OEC

function join_nodelists(
    df_left::AbstractDataFrame,
    df_right::AbstractDataFrame,
)
    innerjoin(df_left, df_right, on=names(df_left)[1], makeunique=true)
end
