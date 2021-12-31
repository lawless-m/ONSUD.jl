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

function index_csv(io, n, kvs, aux, kvs_lk, aux_lk)
    readline(io)
    while ! eof(io)
        pos=position(io)
        uprn = parse(UInt64, readuntil(io, ","))
        lock(kvs_lk)
        try
            kvs[uprn] = pos
        finally
            unlock(kvs_lk)
        end
        lock(aux_lk)
        try
            aux[uprn] = n
        finally
            unlock(aux_lk)
        end
        readline(io)
    end
end

function index_datadir(datadir)
    kvs = Dict{UInt64, UInt64}()
    aux = Dict{UInt64, UInt64}()
    kvs_lk = ReentrantLock()
    aux_lk = ReentrantLock()
    files = readdir(datadir)
    @sync for n in 1:length(files)
        Threads.@spawn open(joinpath(datadir, files[n]), "r") do io
                index_csv(io, n, kvs, aux, kvs_lk, aux_lk)
            end
    end
    files, kvs, aux
end

function record_entry!(ch, kvs, aux)
    t = take!(ch)
    while t !== nothing
        kvs[t[1]] = t[2]
        aux[t[1]] = t[3]
    end
end

function create_index1024(datadir, indexfile)
    files, kvs, aux = index_datadir(datadir)
    meta = [files[i] for i in sort(collect(keys(files)))]
    build_index_file(indexfile, kvs; meta, aux)
end

function uprn_data(idx::Index, datadir, uprn; header=String[])
    node = search(idx, uprn)
    if node === nothing
        return nothing
    end
    local data::DataFrame
    open(joinpath(datadir, idx.meta[node[2]])) do io 
        if length(header) == 0
            header = names(CSV.read(io, DataFrame; limit=0))
        end
        seek(io, node[1])
        data = CSV.read(io, DataFrame; limit=1, header)
    end
    data
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


###
end
