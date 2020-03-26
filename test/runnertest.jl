module runnertest

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
    using LAModelData
    using StayingPutSim
    using CareData
    using ONSCodes

    main_results =  CareData.makecareroutcomesframe(0)

    grantdata = CSV.File( DATADIR*"edited/GRANTS_2019.csv" ) |> DataFrame

    @testset "SP Sim" begin
        alldata = CareData.load_all()
        iterations = 200
        for iteration in 1:iterations
            yp_data = CSV.File( DATADIR*"created_doe/yp_data_$iteration.csv" ) |> DataFrame
            carer_data = CSV.File( DATADIR*"created_doe/carer_data_$iteration.csv" ) |> DataFrame

            rc = size( carer_data )[1]
            ry = size( yp_data )[1]
            @test rc == ry
            params = StayingPutSim.get_default_params()
            outcomes = CareData.CarerOutcomes(0.0,0.0,0.0)
            @time for r in 1:rc
                year = yp_data[r,:year]
                ccode = yp_data[r,:ccode]
                # this test isn't strictly needed since we only create for live councils
                if (! ONSCodes.isaggregate( ccode )) && (! ( ccode in SKIPLIST ))
                    carer = CareData.carerfromrow( carer_data[r,:] )
                    yp = CareData.ypfromrow( yp_data[r,:])
                    @assert carer.id == yp_data[r,:carer]
                    @assert year == carer_data[r,:year]
                    which = alldata.ofdata.ccode .== ccode
                    ofdata = alldata.ofdata[which,:]
                    @assert size(ofdata)[1] == 1
                    ofdatar = ofdata[1,:]
                    # println( ofdata.council )
                    if ccode in keys( FUNCS )
                        outcomes = FUNCS[ccode](
                            ccode,
                            year,
                            carer,
                            yp,
                            ofdatar,
                            params )
                    else
                        outcomes = StayingPutSim.do_basic_calc(
                            ccode,
                            year,
                            carer,
                            yp,
                            ofdatar,
                            params )
                    end
                    CareData.addcareroutcomestoframe!(
                        main_results,
                        1,
                        iteration,
                        year,
                        ccode,
                        carer.id,
                        outcomes )
                    if r % 100 == 0
                        println( carer.id )
                    end
                end # good code check
            end # main loop
        end # iteration
        CSVFiles.save( DATADIR*"created_doe/main_results2.csv", main_results )

        by_la_sys_iteration_and_year = main_results |>
            @filter( _.year > 2018 && _.year < 2025 ) |>
            @groupby( [_.ccode,  _.year, _.sysno, _.iteration ] ) |>
            @map({
                index=key(_),
                year=first(_.year),
                ccode=first(_.ccode),
                sysno=first(_.sysno),
                iteration=first(_.iteration),
                cnt=length( _ ),
                income=sum( _.income_recieved )*52.0,
                contribs = sum( _.contributions_from_yp )*52.0,
                payments = sum( _.payments_from_la )*52.0
                }

                ) |>
            @orderby( [_.year,_.ccode,_.sysno, _.iteration ] ) |>
            DataFrame
        CSVFiles.save( DATADIR*"created_doe/by_la_sys_iteration_and_year.csv", by_la_sys_iteration_and_year, delim='\t' )
        show( by_la_sys_iteration_and_year )

        m1 = by_la_sys_iteration_and_year |> @join( alldata.grantdata, _.ccode, _.ccode, { _.year, __.ccode, _.payments, __.amount } ) |> DataFrame

        by_la_sys_iteration_and_year = CSV.File( DATADIR*"created_doe/by_la_sys_iteration_and_year.csv", delim='\t' ) |> DataFrame
        by_la_sys_and_year=by_la_sys_iteration_and_year |>
            @groupby( [_.ccode,  _.year, _.sysno] ) |>
            @map(
                    {
                       ccode  = first(_.ccode),
                       year   = first(_.year),
                       sysno  = first(_.sysno),

                       avg_cnt=mean( _.cnt  ),
                       min_cnt=minimum( _.cnt ),
                       max_cnt=maximum( _.cnt ),
                       pct_10_cnt=quantile( _.cnt, [0.10] )[1],
                       pct_25_cnt=quantile( _.cnt, [0.25] )[1],
                       pct_75_cnt=quantile( _.cnt, [0.75] )[1],
                       pct_90_cnt=quantile( _.cnt, [0.90] )[1],

                       avg_income=mean( _.income  ),
                       min_income=minimum( _.income ),
                       max_income=maximum( _.income ),
                       pct_10_income=quantile( _.income, [0.10] )[1],
                       pct_25_income=quantile( _.income, [0.25] )[1],
                       pct_75_income=quantile( _.income, [0.75] )[1],
                       pct_90_income=quantile( _.income, [0.90] )[1],

                       avg_contribs = mean( _.contribs ),
                       min_contribs = minimum( _.contribs ),
                       max_contribs = maximum( _.contribs ),
                       pct_10_contribs=quantile( _.contribs, [0.10] )[1],
                       pct_25_contribs=quantile( _.contribs, [0.25] )[1],
                       pct_75_contribs=quantile( _.contribs, [0.75] )[1],
                       pct_90_contribs=quantile( _.contribs, [0.90] )[1],

                       avg_payments = mean( _.payments  ),
                       min_payments = minimum( _.payments ),
                       max_payments = maximum( _.payments ),
                       pct_10_payments=quantile( _.payments, [0.10] )[1],
                       pct_25_payments=quantile( _.payments, [0.25] )[1],
                       pct_75_payments=quantile( _.payments, [0.75] )[1],
                       pct_90_payments=quantile( _.payments, [0.90] )[1]
                    }
                ) |>
            @orderby( [ _.ccode, _.year,_.sysno] ) |>
            DataFrame

            merged = join( by_la_sys_and_year, grantdata, on=:ccode, makeunique=true )

        CSVFiles.save( DATADIR*"created_doe/by_la_sys_and_year.csv", by_la_sys_and_year, delim='\t' )
        CSVFiles.save( DATADIR*"created_doe/by_la_sys_and_year_merged_with_grant.csv", merged, delim='\t' )
    end


    @testset "grant aggregation" begin
        grantdata = CSV.File( DATADIR*"edited/GRANTS_2019.csv" ) |> DataFrame
        mg = mergegrantstoregions( grantdata )
        print( mg )
    end

end
