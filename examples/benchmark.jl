using BenchmarkTools
using ONSUD


uprns = map(l->parse(Int, l), readlines("/home/matt/wren/UkGeoData/uprns"))

uprnidx = ONSUD.Index1024.open_index("/home/matt/wren/UkGeoData/onsud_nov_2021.uprn.index")

function srch()
    for u in uprns
        ONSUD.Index1024.search(uprnidx, u)
    end
end

#===
pre aligned - 1_332_911_592 onsud_nov_2021.uprn.index

julia> @benchmark srch()
BenchmarkTools.Trial: 1039 samples with 1 evaluation.
 Range (min … max):  4.565 ms …   9.890 ms  ┊ GC (min … max): 0.00% … 49.30%
 Time  (median):     4.696 ms               ┊ GC (median):    0.00%
 Time  (mean ± σ):   4.810 ms ± 668.742 μs  ┊ GC (mean ± σ):  2.15% ±  7.17%

 Memory estimate: 1.17 MiB, allocs estimate: 33286.

with alignment -  2_708_139_112  onsud_nov_2021.uprn.index

julia> @benchmark srch()
BenchmarkTools.Trial: 896 samples with 1 evaluation.
 Range (min … max):  4.975 ms … 129.211 ms  ┊ GC (min … max): 0.00% … 95.57%
 Time  (median):     5.455 ms               ┊ GC (median):    0.00%
 Time  (mean ± σ):   5.577 ms ±   4.137 ms  ┊ GC (mean ± σ):  2.47% ±  3.19%

 Memory estimate: 1.17 MiB, allocs estimate: 33286.

So the alignment made things worse, because of the 2x filesize I guess

 ==#


