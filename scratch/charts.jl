using DataFrames
using CSV
using VegaLite

main = CSV.File("/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/mainresults/by_sys_and_year.csv", delim='\t') |> DataFrame

main

function makeplot( main :: DataFrame, measure :: AbstractString )
    titles = []
    v = main |>
           @vlplot(
            title = "Projected Cost of Staying Put, 2019-2024",
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

save( "results/images/main_agg.svg", v )
save( "results/images/main_agg.png", v )
save( "results/images/main_agg.pdf", v )

names( main )

by_la = CSV.File("/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/mainresults/by_la_sys_and_year_merged_with_grant.csv", delim='\t') |> DataFrame
