using DataFrames
using CSV
using VegaLite

main = CSV.File("/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/mainresults/by_sys_and_year.csv", delim='\t') |> DataFrame

main

function makeplot( main :: DataFrame, measure :: AbstractString )

    v = main |>
           @vlplot(
            title = "Projected Total Young People in Staying Put, 2019-2024",
            width=600,
            height=500,
            x={"year:o"}
           ) +
           @vlplot(
               mark={:area,color=:blue,opacity=0.1},
               title="",
               y={"min_payments_sys_1:q", title=""},
               y2={"max_payments_sys_1:q", title=""}
           )  +
           @vlplot(
               mark={:area,color=:blue,opacity=0.1},
               title="",
               y={"pct_10_payments_sys_1:q",title=""},
               y2={"pct_90_payments_sys_1:q",title=""}
           ) +
           @vlplot(
               mark={:area,color=:blue,opacity=0.3},
               title="",
               y={"pct_25_payments_sys_1:q", title=""},
               y2={"pct_75_payments_sys_1:q", title=""}
           ) +
           @vlplot(
               mark={:line,color=:black},
               y="avg_payments_sys_1:q",
           )+
           @vlplot(
               mark={:area,color=:red,opacity=0.1},
               title="",
               y={"min_payments_sys_2:q", title=""},
               y2={"max_payments_sys_2:q", title=""}
           )  +
           @vlplot(
               mark={:area,color=:red,opacity=0.1},
               title="",
               y={"pct_20_payments_sys_2:q",title=""},
               y2={"pct_90_payments_sys_2:q",title=""}
           ) +
           @vlplot(
               mark={:area,color=:red,opacity=0.3},
               title="",
               y={"pct_25_payments_sys_2:q", title=""},
               y2={"pct_75_payments_sys_2:q", title=""}
           ) +
           @vlplot(
               mark={:line,color=:black},
               y="avg_payments_sys_2:q",
           ) +
           @vlplot(
               mark={:area,color=:green,opacity=0.1},
               title="",
               y={"min_payments_sys_3:q", title=""},
               y2={"max_payments_sys_3:q", title=""}
           )  +
           @vlplot(
               mark={:area,color=:green,opacity=0.1},
               title="",
               y={"pct_30_payments_sys_3:q",title=""},
               y2={"pct_90_payments_sys_3:q",title=""}
           ) +
           @vlplot(
               mark={:area,color=:green,opacity=0.3},
               title="",
               y={"pct_35_payments_sys_3:q", title=""},
               y2={"pct_75_payments_sys_3:q", title=""}
           ) +
           @vlplot(
               mark={:line,color=:black},
               y="avg_payments_sys_3:q",
           )
    v
end

function makeplot2( main :: DataFrame, measure :: AbstractString )

    colours = [:red, :blue, :green, :orange, :purple]
    titles = [
    "current modelled",
    "option 2(a), with Oldham style fee",
    "option 2(a), without fee",
    "option 2(b), with HB from all benefit recipients",
    "option 3 - 2(a) with taper"
    ]
    v = main |>
           @vlplot(
            title = "Projected Annual Costs to Local Authorities of Staying Put, 2019-2024",
            width=600,
            height=500,
            legend= { :title="" },
            x={"year:o"}
           )
    for sys in 1:5
        v +=
           @vlplot(
               mark={:area,color=colours[sys],opacity=0.1},
               title="",
               y={"min_payments_sys_$sys:q", title=""},
               y2={"max_payments_sys_$sys:q", title=""}
           )  +
           @vlplot(
               mark={:area,color=colours[sys],opacity=0.1},
               title="",
               y={"pct_10_payments_sys_$sys:q",title=""},
               y2={"pct_90_payments_sys_$sys:q",title=""}
           ) +
           @vlplot(
               mark={:area,color=colours[sys],opacity=0.1},
               title="",
               y={"pct_25_payments_sys_$sys:q", title=""},
               y2={"pct_75_payments_sys_$sys:q", title=""}
           ) +
           @vlplot(
               mark={:line,color=colours[sys]},
               y={"avg_payments_sys_$sys:q", label=titles[sys]}
           )
       end
    v
end

save( "results/images/main_agg_2.svg", v )
save( "results/images/main_agg_2.png", v )
save( "results/images/main_agg_2.pdf", v )

names( main )

by_la = CSV.File("/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/mainresults/by_la_sys_and_year_merged_with_grant.csv", delim='\t') |> DataFrame

main = CSV.File("/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/mainresults/by_sys_and_year.csv", delim='\t') |> DataFrame
