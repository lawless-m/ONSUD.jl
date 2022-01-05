using ONSUD
using Test

function buff(size=3000)
    io = IOBuffer()
    write(io, [UInt8(1) for _ in 1:size])
    seek(io, 0)
    io
end

function testdata(testdir)
    if !isdir(testdir)
        println(stderr, "TestData not present, so skipping")
        return true
    end

    @time create_index1024(testdir, "/tmp/onsud_test.index", ONSUD.by_uprn!)
    uprn_data(testdir, "/tmp/onsud_test.index", 10013555700) !== nothing
end

function test_generate(testdir)
    if !isdir(testdir)
        println(stderr, "TestData not present, so skipping")
        return true
    end
    ONSUD.save(generate(ONSUD.row_readers(testdir)), buff())
end

@testset "ONSUD.jl" begin
    @test isa(ONSUD.UPRNDB(), ONSUD.UPRNDB)
    @test isa(test_generate("/home/matt/wren/UkGeoData/ONSUD_NOV_2021/Test"), ONSUD.UPRNDB)
    @test testdata("/home/matt/wren/UkGeoData/ONSUD_NOV_2021/Test")
end
