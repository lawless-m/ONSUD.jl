module ONSUD

using ZipFile
using CSV
using AODictionary
using Serialization
using Index1024
using DataFrames

export check_for_update

export generate

export uprn_data, create_index1024, index_by_postcode, open_pcodedb, pcode_info

const mask = 0xffff000000000000 # 64k files should be enough for anybody
const shift = 48

const ENDim = typeof((e=zero(UInt64), n=zero(UInt64), dim=zero(UInt64)))
const EN = typeof((e=zero(Int64), n=zero(Int64)))

tag(n, v) = UInt64(v) | (UInt64(n) << shift)
tag(v::UInt64) = UInt32((UInt64(v) & mask) >> shift)
key(n::Index1024.NodeInfo) = key(n.tagged_key)
key(v::UInt) = UInt64(v) & ~mask

const Grid = NamedTuple{(:e, :n, :pc), Tuple{UInt64, UInt64, UInt64}}

const GEODIR = "/home/matt/wren/UkGeoData"
const DATADIR = joinpath(GEODIR, "ONSUD_NOV_2021/Data")

struct UPRNDB
    grid::Dict{Int64, Grid}
    field2uprn::Dict{Symbol, Dict{String, Vector{Int64}}}
    uprn2dimension::Dict{Int64, UInt64}
    dimensions::Dict
    UPRNDB() = new(Dict{Int64, Grid}(), Dict{Symbol, Dict{String, Vector{Int64}}}(), Dict{Int64, UInt64}(), Dict())
end

function fill_fields!(db::UPRNDB, row)
    for field in propertynames(row)
        if ! (field in [:uprn, :gridgb1e, :gridgb1n, :pcds])
            db.field2uprn[field] = Dict{String, Vector{Int64}}()
        end
    end
end

str(v) = ismissing(v) ? "missing" : String(v)

function dimension(db::UPRNDB, row)
    if length(db.field2uprn) == 0
        fill_fields!(db, row)
    end

    dim = Dict{Symbol, String}()
    for k in keys(db.field2uprn)
        dim[Symbol(k)] = str(row[k])
    end
    (;dim...)
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
    db.grid[uprn] = (e=parse(UInt64, row.gridgb1e), n=parse(UInt64, row.gridgb1n), pc=postcode_to_UInt64(row.pcds))
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
    foreach((i,s)->info[s] = dim[i], enumerate(keys(db.field2uprn)))
    info
end

check_for_update() = println("Visit https://geoportal.statistics.gov.uk/search?sort=-created&tags=onsud")
    
zipped_row_readers(zipfile) = map(file->(file.name, ()->CSV.Rows(read(file))), filter(f->startswith(f.name, "Data/") && endswith(f.name, ".csv"), ZipFile.Reader(zipfile).files))
row_readers(datadir) = map(n->(n, ()->CSV.Rows(read(joinpath(datadir, n)))), readdir(datadir))

# @pipe ONSUD.row_readers(ONSUD.DATADIR) |> generate |> save(joinpath(ONSUD.GEODIR, "uprndb.db"), _)

function generate(reader::Tuple)
    println(reader[1])
    db = UPRNDB()
    foreach(row->add!(db, row), reader[2]())
    db 
end

function generate(readers)
    dbs = Vector{UPRNDB}(undef, length(readers))
    Threads.@threads for i in 1:length(readers)
        dbs[i] = generate(readers[i])
    end
    uprndb = UPRNDB()
    for db in dbs
        merge!(uprndb.grid, db.grid)
        merge!(uprndb.field2uprn, db.field2uprn)
        merge!(uprndb.uprn2dimension, db.uprn2dimension)
        merge!(uprndb.dimensions, db.dimensions)
    end
    uprndb
end


function save(io::IO, object)
    serialize(io, object)
    object
end

function save(memofile::AbstractString, object)
    try
        open(memofile, "w+") do io
           return save(io, object)
        end
    catch e
        println(stderr, e)
    end
    object
end

function load(memofile)
    if filesize(memofile) > 0
        open(memofile, "r") do io
            return deserialize(io)
        end
    end
end

############### Index1024 stuff

function postcode_to_UInt64(pc) 
    if ismissing(pc)
        return 0
    end
    reduce((a,c) -> UInt64(a) << 8 + UInt8(c), filter(c->c != ' ', collect(pc)), init=0)
end

function pc_index(db::UPRNDB)
    pcds = Dict{UInt64, Set{ENDim}}()
    for uprn in keys(db.grid)
        (;e,n,pc) =  db.grid[uprn]
        dim = db.uprn2dimension[uprn]
        if ! (pc in keys(pcds))
            pcds[pc] = Set{ENDim}([(;e,n,dim)])
        else
            push!(pcds[pc], (;e,n,dim))
        end
    end
    pcds
end

Base.write(io::IO, endim::ENDim) = write(io, endim.e) + write(io, endim.n) + write(io, endim.dim)
Base.read(io::IO, ::Type{ENDim}) = (e=read(io, UInt64), n=read(io, UInt64), dim=read(io, UInt64))
Base.write(io::IO, endims::Set{ENDim}) = reduce((a,endim)->a+write(io, endim), endims, init=write(io, UInt64(length(endims))))
Base.read(io::IO, ::Type{Set{ENDim}}) = Set{ENDim}([read(io, ENDim) for i in 1:read(io, UInt64)])

function write_and_index_pcode_data(io::IO, pcds)
    kvs = Dict{UInt64, Index1024.DataAux}()
    for pc in keys(pcds)
        kvs[pc] = (data=position(io), aux=length(pcds[pc])) # may as well put something in aux
        write(io, pcds[pc])
    end
    kvs
end

#==
    index_by_postcode("nov_2021.uprndb")
    pdb = ONSUD.open_pcodedb(joinpath(ONSUD.GEODIR, "pcode.db.index"))
    pcode_info(pdb, "S17 3BB")
    
    index_by_postcode("test.uprndb"; pcindexfile="pctest.db.index", datadir="ONSUD_NOV_2021/Test")
    pdb = ONSUD.open_pcodedb(joinpath(ONSUD.GEODIR, "pctest.db.index"))
    pcode_info(pdb, "S17 3BB")

    
    index_by_postcode("bbtest.uprndb"; pcindexfile="bbtest.db.index", datadir="ONSUD_NOV_2021/BB")
    pdb = ONSUD.open_pcodedb(joinpath(ONSUD.GEODIR, "bbtest.db.index"))
    pcode_info(pdb, "S17 3BB")
==#

function index_by_postcode(uprndbfile::AbstractString; pcindexfile="pcode.db.index", geodir="", datadir="")
    if geodir == ""
        geodir = ONSUD.GEODIR
    end
    if datadir == ""
        datadir = ONSUD.DATADIR
    end
    uprndb = load(joinpath(geodir, uprndbfile))
    if uprndb === nothing
        uprndb = save(joinpath(geodir, uprndbfile), generate(row_readers(joinpath(geodir, datadir))))
    end
    index_by_postcode(uprndb; pcindexfile, geodir)
end

function index_by_postcode(uprndb::UPRNDB; pcindexfile="pcode.db.index", geodir="")
    if geodir == ""
        geodir = ONSUD.GEODIR
    end
    open(joinpath(geodir, pcindexfile), "w+") do io
        write(io, zero(Int64)) # placeholder for index_pos
        write(io, zero(Int64)) # placeholder for dimension_pos
        dimension_pos = position(io)    
        @assert dimension_pos > 0
        save(io, uprndb.dimensions)
        Index1024.nextblock(io)    
        kvs = write_and_index_pcode_data(io, pc_index(uprndb)) # pcode=>(data=offset, aux=dimcount)
        Index1024.nextblock(io)
        index_pos = position(io)    
        @assert index_pos > 0
        build_index_file(io, kvs; meta=String["Self Contained Indexed Database"])
        seekstart(io)
        write(io, index_pos)
        write(io, dimension_pos)
    end
end

function open_pcodedb(pcodefn)
    io = open(pcodefn, "r")
    index_pos = read(io, Int64)
    dimension_pos = read(io, Int64)
    seek(io, dimension_pos)
    dimensions = deserialize(io)
    seek(io, index_pos)
    index = open_index(io)
    (;index, dimensions)
end

pcode_info(pcodefn::AbstractString, pcode) = pcode_info(open_pcodedb(pcodefn), pcode)

function pcode_info(pcodedb::NamedTuple, pcode)
    node = search(pcodedb.index, postcode_to_UInt64(pcode))
    if node === nothing
        return nothing
    end
    seek(pcodedb.index.io, node.data)
    read(pcodedb.index.io, Set{ENDim})
end

function by_uprn!(io, n, lk, kvs)
    readline(io) # header
    while ! eof(io)
        pos = position(io)
        uprn = parse(UInt64, readuntil(io, ","))
        lock(lk)
        try
            kvs[uprn] = (data=pos, aux=n)
        finally
            unlock(lk)
        end
        readline(io)
    end
end

function index_datadir(datadir, index_by!)
    kvs = Dict{UInt64, Index1024.DataAux}()
    lk = ReentrantLock()
    files = readdir(datadir)
    @sync for n in 1:length(files)
        Threads.@spawn open(joinpath(datadir, files[n]), "r") do io
            index_by!(io, n, lk, kvs)
        end
    end
    files, kvs
end

#==
@time ONSUD.create_index1024(ONSUD.DATADIR, "/home/matt/wren/UkGeoData/onsud_nov_2021.uprn.index", by_uprn!)
using BenchmarkTools
@benchmark ONSUD.uprn_data(ONSUD.DATADIR, "/home/matt/wren/UkGeoData/onsud_nov_2021.uprn.index", 10015278860)
==#

function create_index1024(datadir, indexfile, index_by!)
    meta, kvs = index_datadir(datadir, index_by!)
    build_index_file(indexfile, kvs; meta)
end

uprn_data(datadir::AbstractString, indexfile::AbstractString, uprn) = uprn_data(datadir, open_index(indexfile), uprn)

function csv(io::IO, offset)
    buff = IOBuffer()
    write(buff, readline(io; keep=true))    
    seek(io, offset)
    write(buff, readline(io; keep=true))
    seekstart(buff)
    CSV.File(buff)
end

function uprn_data(datadir, idx::Index, uprn)
    node = search(idx, uprn)
    if node === nothing
        return nothing
    end
    open(joinpath(datadir, idx.meta[node.aux])) do io 
        csv(io, node.data)
    end
end

###
end
