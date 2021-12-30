module ONSUD

using ZipFile
using CSV
using AODictionary
using Serialization

export check_for_update

export generate

const Grid = NamedTuple{(:e, :n), Tuple{Int64, Int64}}

struct UPRNDB
    grid::Dict{Int64, Grid}
    field2uprn::AODict{Symbol, Dict{String, Vector{Int64}}}
    uprn2dimension::Dict{Int64, UInt64}
    dimensions::Dict
    UPRNDB() = new(Dict{Int64, Grid}(), AODict{Symbol, Dict{String, Vector{Int64}}}(), Dict{Int64, UInt64}(), Dict())
end

function fill_fields!(db::UPRNDB, row)
    for field in propertynames(row)
        if ! (field in [:uprn, :gridgb1e, :gridgb1n])
            db.field2uprn[field] = Dict{String, Vector{Int64}}()
        end
    end
end

str(v) = ismissing(v) ? "missing" : String(v)

function dimension(db::UPRNDB, row)
    if length(db.field2uprn) == 0
        fill_fields!(db, row)
    end

    map(k->str(getindex(row, k[2])), enumerate(keys(db.field2uprn)))
end

function dimension_index(db::UPRNDB, row)
    dim = dimension(db, row)
    hsh = hash(dim)
    if ! (hsh in keys(db.dimensions))
        db.dimensions[hsh] = dim
    end
    hsh
end

function add!(db::UPRNDB, row)
    uprn = parse(Int, row.uprn)
    db.grid[uprn] = (e=parse(Int64, row.gridgb1e), n=parse(Int64, row.gridgb1n))
    db.uprn2dimension[uprn] = dimension_index(db, row)
    for f in keys(db.field2uprn)
        v = str(row[f])
        if ! (v in keys(db.field2uprn[f]))
            db.field2uprn[f][v] = [uprn]
        else
            push!(db.field2uprn[f][v], uprn)
        end
    end
end

function uprninfo(db::UPRNDB, uprn)
    dim = db.dimensions[db.uprn2dimension[uprn]]
    info = Dict{Symbol, Any}(:grid=>db.grid[uprn])
    for (i,s) in enumerate(keys(db.field2uprn))
        info[s] = dim[i]
    end
    info
end

function check_for_update()
    println("Visit https://geoportal.statistics.gov.uk/search?sort=-created&tags=onsud")
end

function row_readers(zipfile)
    z = ZipFile.Reader(zipfile)
    readers = []
    for (i,file) in enumerate(z.files)
        if startswith(file.name, "Data/") && endswith(file.name, ".csv")
            push!(readers, (file.name, ()->CSV.Rows(read(z.files[i]))))
        end
    end
    readers
end

function generate(zipfile="/home/matt/wren/UkGeoData/ONSUD_NOV_2021.zip", memofile="/home/matt/wren/UkGeoData/ONSUD_NOV_2021.sj")
    readers = row_readers(zipfile)
    db = UPRNDB()
    for (fname, rows) in readers
        println(fname)
        for row in rows()
            add!(db, row)
        end
    end    
    save(db, memofile)
end

function index_csv(io, fileN)
    index = Dict()
    readline(io)
    while ! eof(io)
        uprn = parse(UInt64, readuntil(io, ","))
        index[uprn] = (fileN=fileN, pos=position(io))
        readline(io)
    end
    index
end

function index_datadir(datadir="/home/matt/wren/UkGeoData/ONSUD_NOV_2021/Data")
    indexes = Dict()
    files = Dict()
    for name in readdir(datadir)
        files[length(files)+1] = name
        open(joinpath(datadir, name), "r") do io
            merge!(indexes, index_csv(io, length(files)))
        end
    end
    files, indexes
end

function save(db::UPRNDB, memofile)
    try
        open(memofile, "w+") do io
            serialize(io, db)
        end
    catch e
        println(stderr, e)
    end
    db
end

function load(memofile)
    if filesize(memofile) > 0
        open(memofile, "r") do io
            return deserialize(io)
        end
    end
end

const mask = 0xffff000000000000

tag(t, v) = UInt64(v) | (UInt64(t)<<48)
detag(v) = UInt16((UInt64(v) & mask) >> 48)

###
end
