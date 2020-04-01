using Test
using DataFrames
using CSVFiles
using CSV
using Query
using Statistics
#
# local imports; see ~/.julia/config/startup.jl
#
using GlobalDecls
using LAModelData: get_underlying_data, get_18s_level_from_doe, get_staying_put_rates,
    do_exit_rate_query, get_target_la, get_pay_class_from_region_code
using StayingPutSim
using CareData
using ONSCodes

@testset "LADataTests" begin
    nt = get_underlying_data()
    @test nt.c19_21_2019[1,:cl_all_19to21] == 29930
    print(nt.c19_21_2019[!,:cl_stayput_ffc_20_pc] )
    @test nt.c19_21_2019[164,:cl_stayput_ffc_20_pc] == 48
    @test LAModelData.UDATA.c17_18_2017[1,Symbol("cl_all_17&18")]== 10_710
    @test LAModelData.UDATA.c19_21_2017[164,:cl_stayput_ffc_20] == 6
    # E09000028 is Southwark;
    # E12000007 is London
    @test get_18s_level_from_doe("E09000028") == 37
    @test do_exit_rate_query( 2019, "E09000028", 18 ) == (37,23)
    @test get_target_la( "E09000007", regional ) == "E12000007"
    spr = get_staying_put_rates( "E09000028", local_authority, false )
    println( "spr=$spr")
    @test spr[1] ≈ 23/37 # 18 yos in

end
