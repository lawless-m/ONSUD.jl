using ONSUD
using Test

function buff(size=3000)
    io = IOBuffer()
    write(io, [UInt8(1) for _ in 1:size])
    seek(io, 0)
    io
end

function testdata(;geodir, datadir)
    testdir = joinpath(geodir, datadir)
    if !isdir(testdir)
        println(stderr, "TestData not present, so skipping")
        return true
    end

    @time create_index1024(testdir, "/tmp/onsud_test.index", ONSUD.by_uprn!)
    uprn_data(testdir, "/tmp/onsud_test.index", 10013555700) !== nothing
end

function test_generate(uprndbfile; geodir, datadir)
    testdir = joinpath(geodir, datadir)
    if !isdir(testdir)
        println(stderr, "TestData not present, so skipping")
        return true
    end
    open(joinpath(geodir, uprndbfile), "w+") do io 
        ONSUD.save(io, generate(ONSUD.row_readers(testdir)))
    end
    filesize(joinpath(geodir, uprndbfile)) > 0
end

function test_pcodedb(uprndbfile; pcindexfile, geodir, datadir)
    testdir = joinpath(geodir, datadir)
    if !isdir(testdir)
        println(stderr, "TestData not present, so skipping")
        return true
    end
    index_by_postcode(uprndbfile; pcindexfile, geodir, datadir)
    filesize(joinpath(geodir, pcindexfile)) > 0
end

function test_openpcodedb(pcodefn)
    if ! isfile(pcodefn)
        println(stderr, "Test pcodedb not present, so skipping")
        return true
    end

    index, dimensions =  open_pcodedb(pcodefn)
    isa(index, ONSUD.Index1024.Index) && isa(dimensions, Dict)
end

const geodir = "/home/matt/wren/UkGeoData"
const datadir = "ONSUD_NOV_2021/Test"
const uprndbfile = "test.uprndb"
const pcindexfile = "pcode.testdb.index"

@testset "ONSUD.jl" begin
    @test isa(ONSUD.UPRNDB(), ONSUD.UPRNDB)
    @test testdata(;datadir, geodir)
    @test test_generate(uprndbfile; geodir, datadir)
    @test test_pcodedb(uprndbfile; pcindexfile, geodir, datadir)
    @test test_openpcodedb(joinpath(geodir, pcindexfile))
end
