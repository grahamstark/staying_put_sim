import StatsBase
import Statistics

import FileIO: load
import DataFrames
import Query
import IterableTables
import CSVFiles
import Test
import CSV

using DataFrames
using CareData
using Query
import CareData:
    loadall,
    get_yp, makeypframe, makecarerframe,
    Carer, YP,
    create_base_datasets, add_carer_to_frame!,
    DataSettings, default_data_settings,
    CPIINDEX, AFC_SURVEY_YEAR, THIS_YEAR, uprate,
    CarerOutcomes, addcareroutcomestoframe!, makecareroutcomesframe,
    EmploymentStatus, TrainingOrEmployment, HigherEd, OtherEd, NEET, Unknown,
    DataPublisher

import IteratorInterfaceExtensions
import TableTraits: isiterabletable

import GlobalDecls
import ONSCodes
import Utils
using LAModelData
import LAModelData: get_staying_rates, get18slevelfromdoe

export test1,loadeng

CCODE_SUSSEX="E10000030"

function test1()
    settings = default_data_settings()
    settings.reaching_18s_source = DFE
    cc = LAModelData.get18slevelfromdoe( CCODE_SUSSEX )
    println( "cc=$cc" )
    cc
end

function teststats()
    by_la = created.yp_data |>
        @groupby( [_.ccode,  _.age, _.year ] ) |>
        @map({ccode=key(_),cnt=length( _ )}) |>
        @orderby( _.ccode ) |>
        DataFrame
    by_la2 = created.yp_data |>
        @groupby( [_.ccode, _.year ] ) |>
        @map({ccode=key(_),cnt=length( _ )}) |>
        @orderby( _.ccode ) |>
        DataFrame

        print( by_la )
        print( by_la2 )
end


function testiterate()
    @time for carer_r in eachrow(created.carer_data)
        carer = CareData.carerfromrow( carer_r )
        yp = CareData.get_yp(
            created.yp_data,
            carer_r.year,
            carer,
            created.year_matches[carer_r.year] )
    end
    @time for carer_r in eachrow(created.carer_data)
        carer = CareData.carerfromrow( carer_r )
        yp = CareData.get_yp(
            created.yp_data,
            carer_r.year,
            carer,
            nothing )
    end
end
POPN_MEASURES = [
    :avg_cnt_sys_1,:min_cnt_sys_1,:max_cnt_sys_1,
    :pct_10_cnt_sys_1,:pct_25_cnt_sys_1,:pct_75_cnt_sys_1,
    :pct_90_cnt_sys_1]

function getvals( merged )
    out = Dict()
    for year in 2019:2025
        targets = merged.year .== year
        println( targets )
        out[year] = Dict()
        for p in POPN_MEASURES
            out[year][p] = merged[targets,p]
        end
    end
    out
end

function loadeng()
    output_dir = "/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/main/"
    print( "X")
    merged = CSV.File( output_dir*"by_sys_and_year.csv", delim='\t' )|>DataFrame
    getvals( merged )
end

out = loadeng()
print( out )
