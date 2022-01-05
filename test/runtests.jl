using ONSUD
using Test

function testdata(testdir)
    if !isdir(testdir)
        println(stderr, "TestData not present, so skipping")
        return true
    end

    @time create_index1024(testdir, "/tmp/onsud_test.index")
    uprn_data(testdir, "/tmp/onsud_test.index", 10013555700) !== nothing
end


@testset "ONSUD.jl" begin
    @test isa(ONSUD.UPRNDB(), ONSUD.UPRNDB)
    @test testdata("/home/matt/wren/UkGeoData/ONSUD_NOV_2021/Test")
end
