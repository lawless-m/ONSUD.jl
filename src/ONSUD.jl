module ONSUD

using ZipFile
using CSV
using AODictionary
using Serialization
using Index1024
using DataFrames

export check_for_update

export generate

const mask = 0xffff000000000000 # 64k files should be enough for anybody
const shift = 48

tag(n, v) = UInt64(v) | (UInt64(n) << shift)
tag(v::UInt64) = UInt32((UInt64(v) & mask) >> shift)
key(n::Index1024.NodeInfo) = key(n.tagged_key)
key(v::UInt) = UInt64(v) & ~mask

const Grid = NamedTuple{(:e, :n), Tuple{Int64, Int64}}

const DATADIR = "/home/matt/wren/UkGeoData/ONSUD_NOV_2021/Data"

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

check_for_update() = println("Visit https://geoportal.statistics.gov.uk/search?sort=-created&tags=onsud")
    
zipped_row_readers(zipfile) = map(file->(file.name, ()->CSV.Rows(read(file))), filter(f->startswith(f.name, "Data/") && endswith(f.name, ".csv"), ZipFile.Reader(zipfile).files))
row_readers(datadir) = map(n->(n, ()->CSV.Rows(read(joinpath(datadir, n)))), readdir(datadir))

# @pipe ONSUD.row_readers(ONSUD.DATADIR) |> generate |> save(_, "/home/matt/wren/UkGeoData/uprndb.db")

function generate(readers)
    db = UPRNDB()
    for (fname, rows) in readers
        println(fname)
        for row in rows()
            add!(db, row)
        end
    end
    db
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

############### Index1024 stuff

function index_csv!(io, n, lk, kvs)
    readline(io) # header
    while ! eof(io)
        pos = position(io)
        uprn = parse(UInt64, readuntil(io, ","))
        lock(lk)
        try
            kvs[uprn] = tag(n, pos)
        finally
            unlock(lk)
        end
        readline(io)
    end
end

function index_datadir(datadir)
    kvs = Dict{UInt64, UInt64}()
    lk = ReentrantLock()
    files = readdir(datadir)
    @sync for n in 1:length(files)
        Threads.@spawn open(joinpath(datadir, files[n]), "r") do io
            index_csv!(io, n, lk, kvs)
        end
    end
    files, kvs
end

# @time ONSUD.create_index1024(ONSUD.DATADIR, "/home/matt/wren/UkGeoData/onsud_nov_2021.index")

function create_index1024(datadir, indexfile)
    meta, kvs = index_datadir(datadir)
    build_index_file(indexfile, kvs; meta)
end

uprn_data(indexfile::AbstractString, datadir, uprn) = uprn_data(open_index(indexfile), datadir, uprn)

function csv(io::IO, offset)
    buff = IOBuffer()
    write(buff, readline(io; keep=true))    
    seek(io, offset)
    write(buff, readline(io; keep=true))
    seekstart(buff)
    CSV.File(buff)
end

function uprn_data(idx::Index, datadir, uprn)
    node = search(idx, uprn)
    if node === nothing
        return nothing
    end
    open(joinpath(datadir, idx.meta[tag(node)])) do io 
        csv(io, key(node))
    end
end

###
end
