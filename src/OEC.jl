module OEC

using CSV
using DataFrames
using Dates
using FromDigits
using Glob
using Mmap: mmap
using ZipArchives: ZipReader, zip_names, zip_readentry, zip_openentry

export unzip_sv, date_int_to_date, list_sv_zip_files, transform_sv

const scratchdir = "/scratch/$(get(ENV, "USER", nothing))"

const BOL_data_scratchdir = joinpath(scratchdir, "temp/bi_dpi/data/OEC/bulk/Bill_of_Lading")

const date_int_cols = Tuple(Symbol(c) for c in ("Estimate Arrival Date", "Actual Arrival Date"))

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
    CSV.File(zip_readentry(archive, files[1]))
end

function list_sv_zip_files(path::AbstractString)
    glob("*.[t,c]sv.zip", path)
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

end # module OEC
