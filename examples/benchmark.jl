using BenchmarkTools
using ONSUD


uprns = map(l->parse(Int, l), readlines("/home/matt/wren/UkGeoData/uprns"))

uprndb = open_index("/home/matt/wren/UkGeoData/nov_2021.uprndb")

function srch()
    for u in uprns
        ONSUD.Index1024.search(uprndb, u)
    end
end

