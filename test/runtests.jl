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
    @time create_index1024(testdir, "/tmp/onsud_test.index", ONSUD.by_uprn!)
    uprn_data(testdir, "/tmp/onsud_test.index", 10013555700) !== nothing
end

function test_generate(uprndbfile; geodir, datadir)
    testdir = joinpath(geodir, datadir)
    open(joinpath(geodir, uprndbfile), "w+") do io 
        ONSUD.save(io, generate(ONSUD.row_readers(testdir)))
    end
    filesize(joinpath(geodir, uprndbfile)) > 0
end

function test_pcodedb(uprndbfile; pcindexfile, geodir, datadir)
    index_by_postcode(uprndbfile; pcindexfile, geodir, datadir)
    filesize(joinpath(geodir, pcindexfile)) > 0
end

function test_pcodeinfo(pcodefn, pcode)
    pcode_info(pcodefn, pcode)
end

function test_openpcodedb(pcodefn)
    index =  open_pcode_index(pcodefn)
    isa(index, ONSUD.Index1024.Index) && length(index.meta) > 25
end

const geodir = "/home/matt/wren/UkGeoData"
const datadir = "ONSUD_NOV_2021/Test"
const uprndbfile = "test.uprndb"
const pcindexfile = "pcode.testdb.index"

@testset "ONSUD.jl" begin
    @test UInt64_to_postcode(postcode_to_UInt64("DG1 1NA")) == "DG1 1NA"
    @test UInt64_to_postcode(postcode_to_UInt64("S17 3BB")) == "S17 3BB"
    @test UInt64_to_postcode(postcode_to_UInt64("EC4A 1DT")) == "EC4A 1DT"
    @test UInt64_to_postcode(postcode_to_UInt64("W1G 8QJ")) == "W1G 8QJ"
   # @test en2lalo(1,2) == [-7.557148076401367, 49.766825796535805]
   # @test en2lalo(426642.0, 380231.0) == [-1.601536798482117, 53.31834270000983]
    if isdir(geodir) && isdir(joinpath(geodir, datadir))
        @test testdata(;datadir, geodir)
        @test test_generate(uprndbfile; geodir, datadir)
        @test test_pcodedb(uprndbfile; pcindexfile, geodir, datadir)
        @test test_openpcodedb(joinpath(geodir, pcindexfile))
#        @test test_pcodeinfo(joinpath(geodir, pcindexfile), "S17 3BB") == [(e = 429573, n = 379050, imd19ind = "28915", lad21cd = "E08000019", bua11cd = "E34999999", pfa19cd = "E23000011", ruc11ind = "C1", oa11cd = "E00040106", oac11ind = "1C3", itl21cd = "E08000019", msoa11cd = "E02001678", pcon18cd = "E14000922", lep17cd1 = "E37000040", cty21cd = "E11000003", ttwa15cd = "E30000261", wz11cd = "E33011399", buasd11cd = "E35999999", ccg19cd = "E38000146", lsoa11cd = "E01007926", npark16cd = "E99999999", ctry191cd = "E92000001", parncp19cd = "E43000173", eer17cd = "E15000003", rgn17cd = "E12000003", ced17cd = "E99999999", wd19cd = "E05010865", hlth19cd = "E18000003", lep17cd2 = "missing"), (e = 429628, n = 379065, imd19ind = "28915", lad21cd = "E08000019", bua11cd = "E34999999", pfa19cd = "E23000011", ruc11ind = "C1", oa11cd = "E00040106", oac11ind = "1C3", itl21cd = "E08000019", msoa11cd = "E02001678", pcon18cd = "E14000922", lep17cd1 = "E37000040", cty21cd = "E11000003", ttwa15cd = "E30000261", wz11cd = "E33011399", buasd11cd = "E35999999", ccg19cd = "E38000146", lsoa11cd = "E01007926", npark16cd = "E99999999", ctry191cd = "E92000001", parncp19cd = "E43000173", eer17cd = "E15000003", rgn17cd = "E12000003", ced17cd = "E99999999", wd19cd = "E05010865", hlth19cd = "E18000003", lep17cd2 = "missing"), (e = 429355, n = 378996, imd19ind = "28915", lad21cd = "E08000019", bua11cd = "E34999999", pfa19cd = "E23000011", ruc11ind = "C1", oa11cd = "E00040106", oac11ind = "1C3", itl21cd = "E08000019", msoa11cd = "E02001678", pcon18cd = "E14000922", lep17cd1 = "E37000040", cty21cd = "E11000003", ttwa15cd = "E30000261", wz11cd = "E33011399", buasd11cd = "E35999999", ccg19cd = "E38000146", lsoa11cd = "E01007926", npark16cd = "E99999999", ctry191cd = "E92000001", parncp19cd = "E43000173", eer17cd = "E15000003", rgn17cd = "E12000003", ced17cd = "E99999999", wd19cd = "E05010865", hlth19cd = "E18000003", lep17cd2 = "missing")]
    end
end
