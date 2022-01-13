module ONSUD

using ZipFile
using CSV
using Serialization
using Index1024
using DataFrames
using Proj4

export check_for_update

export generate

export uprn_data, create_index1024, index_by_postcode, open_pcode_index, pcode_info, save, load
export postcode_to_UInt64, UInt64_to_postcode, en2lalo, lalo2en

const mask = 0xffff000000000000 # 64k files should be enough for anybody
const shift = 48

const LaLoDim = typeof((la=0.0, lo=0.0, dim=zero(UInt64)))
const LaLo = typeof((la=0.0, lo=0.0))


#const osgb36 = Proj4.epsg[27700]  # magic number from Proj4
#const latlong = Proj4.epsg[4326]  # magic number from Proj4
#en2lalo(e, n) =  Proj4.transform(Projection(osgb36), Projection(latlong) , [e, n])
#lalo2en(la, lo) = Proj4.transform(Projection(latlong), Projection(osgb36) , [la, lo])

#==
en2lalo(e, n) = Proj4.Transformation("EPSG:27700", "EPSG:4326", always_xy=true)((e,n))
lalo2en(la, lo) = Proj4.Transformation("EPSG:4326", "EPSG:27700", always_xy=true)((la, lo))
==#

#==
osgb36 = Projection(Proj4.epsg[27700])  # magic number from Proj4
latlong = Projection(Proj4.epsg[4326])  # magic number from Proj4
en2lalo(e, n) =  Proj4.transform(osgb36 ,latlong , [e, n])
==#

tag(n, v) = UInt64(v) | (UInt64(n) << shift)
tag(v::UInt64) = UInt32((UInt64(v) & mask) >> shift)
key(n::Index1024.NodeInfo) = key(n.tagged_key)
key(v::UInt) = UInt64(v) & ~mask

const Grid = NamedTuple{(:la, :lo, :pc), Tuple{Float64, Float64, UInt64}}

const GEODIR = "/home/matt/wren/UkGeoData"
const DATADIR = joinpath(GEODIR, "ONSUD_NOV_2021/Data")

const Field = Symbol

struct UPRNDB
    grid::Dict{Int64, Grid}
    field2uprn::Dict{Field, Dict{String, Vector{Int64}}}
    uprn2dimension::Dict{Int64, UInt64}
    dimensions::Dict
    UPRNDB() = new(Dict{Int64, Grid}(), Dict{Field, Dict{String, Vector{Int64}}}(), Dict{Int64, UInt64}(), Dict())
end 

str(v) = ismissing(v) ? "missing" : String(v)

function dimension(db::UPRNDB, row)
    dim = Dict{Field, String}()
    for k in keys(db.field2uprn)
        dim[Field(k)] = str(row[k])
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

function add!(db::UPRNDB, row, en2lalo)
    uprn = parse(Int, row.uprn)
    e = parse(UInt64, row.gridgb1e)
    n = parse(UInt64, row.gridgb1n)
    la, lo = en2lalo((e, n))
    db.grid[uprn] = (la=la, lo=lo, pc=postcode_to_UInt64(row.pcds))
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
    if uprn in keys(db.grid)
        (grid=db.grid[uprn], db.dimensions[db.uprn2dimension[uprn]]...)
    end
end

check_for_update() = println("Visit https://geoportal.statistics.gov.uk/search?sort=-created&tags=onsud")
    
zipped_row_readers(zipfile) = map(file->(file.name, ()->CSV.Rows(read(file))), filter(f->startswith(f.name, "Data/") && endswith(f.name, ".csv"), ZipFile.Reader(zipfile).files))
row_readers(datadir) = map(n->(n, ()->CSV.Rows(read(joinpath(datadir, n)))), readdir(datadir))

# @pipe ONSUD.row_readers(ONSUD.DATADIR) |> generate |> ONSUD.save(joinpath(ONSUD.GEODIR, "nov_2021.uprndb"), _)

function generate(reader::Tuple)
    println(reader[1])
    en2lalo = Proj4.Transformation("EPSG:27700", "EPSG:4326", always_xy=true)
    db = UPRNDB()
    rows = reader[2]()
    foreach(f->db.field2uprn[f] = Dict{String, Vector{Int64}}(), filter(f->!(f in [:uprn, :gridgb1e, :gridgb1n, :pcds]), rows.names))
    foreach(row->add!(db, row, en2lalo), rows)
    db 
end

function generate(readers)
    dbs = Vector{UPRNDB}(undef, length(readers))
    
    Threads.@threads for i in 1:length(readers)
        dbs[i] = generate(readers[i])
    end
    println(stderr, "generated")
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
    m = match(r"([A-Z]+)([0-9]+([A-Z]+)?) ?([0-9]+)([A-Z]+)", replace(pc, " "=>""))
    if m == nothing || m[1] === nothing || m[2] === nothing || m[4] === nothing || m[5] === nothing
        return 0
    end
    reduce((a,c) -> UInt64(a) << 8 + UInt8(c), collect(lpad(m[1], 2) * lpad(m[2], 2) * m[4] * m[5]), init=0)
end

function UInt64_to_postcode(u)
    if u == 0
        return ""
    end
    cs = Char[]
    while u > 0
        push!(cs, Char(u & 0xff))
        u >>= 8
    end
    part(cs) = replace(String(reverse(cs)), " "=>"")
    "$(part(cs[4:end])) $(part(cs[1:3]))"
end

function pc_index(db::UPRNDB, dim_positions)
    pcds = Dict{UInt64, Set{LaLoDim}}()
    for uprn in keys(db.grid)
        (;la,lo,pc) =  db.grid[uprn]
        dim = dim_positions[db.uprn2dimension[uprn]]
        if !haskey(pcds, pc)
            pcds[pc] = Set{LaLoDim}([(;la,lo,dim)])
        else
            push!(pcds[pc], (;la,lo,dim))
        end
    end
    pcds
end

Base.write(io::IO, lalodim::LaLoDim) = write(io, lalodim.la) + write(io, lalodim.lo) + write(io, lalodim.dim)
Base.read(io::IO, ::Type{LaLoDim}) = (la=read(io, Float64), lo=read(io, Float64), dim=read(io, UInt64))
Base.write(io::IO, lalodims::Set{LaLoDim}) = reduce((a,lalodim)->a+write(io, lalodim), lalodims, init=write(io, UInt64(length(lalodims))))
Base.read(io::IO, ::Type{Set{LaLoDim}}) = Set{LaLoDim}([read(io, LaLoDim) for i in 1:read(io, UInt64)])

Base.write(io::IO, lalo::LaLo) = write(io, lalo.la) + write(io, lalo.lo)
Base.read(io::IO, ::Type{LaLo}) = (la=read(io, Float64), lo=read(io, Float64))
Base.write(io::IO, lalos::Set{LaLo}) = reduce((a,lalo)->a+write(io, lalo), lalos, init=write(io, UInt64(length(lalos))))
Base.read(io::IO, ::Type{Set{LaLo}}) = Set{LaLo}([read(io, LaLo) for i in 1:read(io, UInt64)])

function write_and_index_pcode_data(io::IO, pcds)
    kvs = Dict{UInt64, Index1024.DataAux}()
    for pc in keys(pcds)
        kvs[pc] = (data=position(io), aux=length(pcds[pc])) # may as well put something in aux
        write(io, pcds[pc])
    end
    kvs
end

function write_dimensions(io::IO, uprndb::UPRNDB)
    fields = sort(collect(keys(uprndb.field2uprn)))
    write(io, length(fields))
    dim_positions = Dict{UInt64, UInt64}()
    for (hsh, dim) in uprndb.dimensions
        dim_positions[hsh] = position(io)
        foreach(f->println(io, dim[f]), fields)
    end
    dim_positions
end

#==
    index_by_postcode("nov_2021.uprndb")
    pdb = ONSUD.open_pcode_index(joinpath(ONSUD.GEODIR, "pcode.db.index"))
    pcode_info(pdb, "S17 3BB")
    
    index_by_postcode("test.uprndb"; pcindexfile="pctest.db.index", datadir="ONSUD_NOV_2021/Test")
    pdb = ONSUD.open_pcode_index(joinpath(ONSUD.GEODIR, "pctest.db.index"))
    pcode_info(pdb, "S17 3BB")

    index_by_postcode("bbtest.uprndb"; pcindexfile="bbtest.db.index", datadir="ONSUD_NOV_2021/BB")
    pdb = ONSUD.open_pcode_index(joinpath(ONSUD.GEODIR, "bbtest.db.index"))
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
        dim_positions = write_dimensions(io, uprndb)
        kvs = write_and_index_pcode_data(io, pc_index(uprndb, dim_positions)) # pcode=>(data=offset, aux=dimcount)
        index_pos = position(io)    
        @assert index_pos > 0
        build_index_file(io, kvs; meta=map(String, sort(collect(keys(uprndb.field2uprn)))))
        seekstart(io)
        write(io, index_pos)
        write(io, dimension_pos)
    end
end

function open_pcode_index(pcodefn)
    io = open(pcodefn, "r")
    index_pos = read(io, Int64)
    seek(io, index_pos)
    open_index(io)
end

pcode_info(pcodefn::AbstractString, pcode) = pcode_info(open_pcode_index(pcodefn), pcode)

function pcode_info(pcodedb::Index, pcode)
    node = search(pcodedb, postcode_to_UInt64(pcode))
    if node === nothing
        return nothing
    end
    seek(pcodedb.io, node.data)
    lalodims = read(pcodedb.io, Set{LaLoDim})
    dim0 = (;Dict([Symbol(f)=>"" for f in pcodedb.meta])...)
    dims = Dict{Int64, typeof(dim0)}()
    function get_dim(dp)
        if !haskey(dims, dp)
            seek(pcodedb.io, dp)
            dims[dp] = (;Dict{Symbol, String}([Symbol(f)=>readline(pcodedb.io) for f in pcodedb.meta])...)
        end
        dims[dp]
    end
    [(la=lalodim.la, lo=lalodim.lo, get_dim(lalodim.dim)...) for lalodim in lalodims]
end

function pcode_inbound_info(pcodedb::Index, pcode)
    min_p = postcode_to_UInt64("$pcode 1AA")
    max_p = postcode_to_UInt64("$pcode 9ZZ")
    if min_p === nothing && max_p === nothing
        println(stderr, "Inbound postcode not found $pcode")
        return
    end
    nodes = Index1024.node_range(pcodedb, min_p, max_p)
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
@time ONSUD.create_index1024(ONSUD.DATADIR, "/home/matt/wren/UkGeoData/onsud_nov_2021.uprn.index", ONSUD.by_uprn!)
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
