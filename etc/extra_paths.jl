## fixme ** should work
# or add_dirs in project
VW_DIR="/home/graham_s/VirtualWorlds/projects/"

STB_DIR=pwd() # joinpath(VW_DIR,"action_for_children/england/")
for dr in ["src/","test"]
    push!(LOAD_PATH, joinpath(STB_DIR,dr))
end
using Revise
