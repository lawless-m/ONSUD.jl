using Documenter
using ONSUD
using Dates


makedocs(
    modules = [ONSUD],
    sitename="ONSUD.jl", 
    authors = "Matt Lawless",
    format = Documenter.HTML(),
)

deploydocs(
    repo = "github.com/lawless-m/ONSUD.jl.git", 
    devbranch = "main",
    push_preview = true,
)
