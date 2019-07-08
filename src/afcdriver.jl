using DataFrames
using FileIO: load
using Query
# using JuliaDB
# local loads
import Utils: averagegain
import GlobalDecls: DATADIR
import StayingPutSim: annualise, maincalc, create_base_datasets, getdefaultparams
import CareData: Carer, YP, create_base_datasets, DataSettings, getyp, makeypframe,
    makecarerframe,  DataSettings

params = getdefaultparams()

all_costs = maincalc( ofdata, params )

gainers = counter(all_costs[:reform_1])
total_carers = size(all_costs)[1]
current_cost=annualise(sum(all_costs[:payments]))
current_incomes=annualise(sum(all_costs[:incomes]))
reform_cost=annualise(sum(all_costs[:reform_1]))
ag = averagegain( all_costs[:reform_1])
println( "gainers=$gainers\n total_carers=$total_carers\n current_cost(m)=$current_cost\n reform_cost(m)=$reform_cost\n average gain=$ag")

# assume:
# * same payments in LA for LA and other carers
# *

by_council = all_costs |>
         @groupby( _.ccode ) |>
         @map({ ccode=key(_),modeltotal=sum(_.payments )*52 }) |>
         @orderby( _.ccode ) |> DataFrame


by_council[:modeltotal] = Int.(trunc.(by_council[:modeltotal]))

merged = join(by_council, grantdata, on=:ccode, makeunique=true )

merged[:modeltotal] = Int.(merged[:modeltotal])
merged[:pctdiff]=100*(merged[:modeltotal]-merged[:amount])./merged[:amount]

merged[[:council,:ccode,:modeltotal,:amount,:pctdiff]]

Gadfly.plot(merged, x=:modeltotal, y=:amount, Geom.point)
