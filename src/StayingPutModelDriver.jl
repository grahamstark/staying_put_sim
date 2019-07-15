module StayingPutModelDriver

    using DataFrames
    using CSVFiles
    using CSV
    using Query
    using Statistics
    using Base.Filesystem
    import IteratorInterfaceExtensions
    import TableTraits: isiterabletable
    #
    #
    using GlobalDecls
    using LAModelData
    using StayingPutSim
    using CareData
    using ONSCodes
    using Parameters

    export doonerun, createmaintables, createnglandtablesbyage, addallgrantcols!, cleanup_main_frame
    POPN_MEASURES = [
        :avg_cnt_sys_1,:min_cnt_sys_1,:max_cnt_sys_1,
        :pct_10_cnt_sys_1,:pct_25_cnt_sys_1,:pct_75_cnt_sys_1,
        :pct_90_cnt_sys_1]


    function add_modelled_grants_to_national!( natdata :: DataFrame, popnname :: AbstractString)
        popcol = Symbol( popnname )
        grantcol = Symbol( popnname * "_grant")
        grant = 23_765_998.0
        # natdata[1,:].amount
        # grant = uprate( grant, 2019, AFC_SURVEY_YEAR ).*1.02 # hacked 1 year inflation, 1 year growth
        popn = natdata[popcol]
        natdata[grantcol] = trackseries(
            grant,#    :: Real,
            popn ) #  :: Vector,
    end

    function add_modelled_grants_to_la!( by_la :: DataFrame, popnname :: AbstractString )
        popcol = Symbol( popnname )
        grantcol = Symbol( popnname * "_grant")
        nrows = size( by_la )[1]
        by_la[grantcol] = zeros(nrows)
        ccodes = unique( by_la.ccode )
        i = 1
        for ccode in ccodes
            which = (by_la.ccode .== ccode)
            later = by_la[which,:]
            lasize = size( later )[1]
            grant = later[1,:].amount
            # grant = uprate( grant, 2019, AFC_SURVEY_YEAR ).*1.02 # hacked 1 year inflation, 1 year growth
            popn = later[popcol]
            tracked_grant = trackseries(
                grant,#    :: Real,
                popn ) #  :: Vector,
                #base_period :: Integer = 1,
                #base_year   :: Integer = AFC_SURVEY_YEAR
            # println( "ccode=$ccode tracked_grant=$tracked_grant")
            j = i+lasize-1
            by_la[i:j,grantcol] = tracked_grant
            i += lasize
        end
    end


    function cleanup_main_frame( by_la :: DataFrame )
        newframe=DataFrame()
        newframe[:Year]=by_la[:year]
        newframe[:Council]=by_la[:council]
        newframe[:Number]=round.(Integer,by_la[:avg_cnt_sys_1])
        newframe[:Grants_Option_1] = round.(Integer,by_la[:avg_cnt_sys_1_grant])
        newframe[:Incomes_Option_1] = round.(Integer,by_la[:avg_income_sys_1])
        newframe[:Incomes_Option_2a_1] = round.(Integer,by_la[:avg_income_sys_2])
        newframe[:Payments_Option_2a_1] = round.(Integer,by_la[:avg_payments_sys_2])
        newframe[:Incomes_Option_2a_2] = round.(Integer,by_la[:avg_income_sys_3])
        newframe[:Payments_Option_2a_2] = round.(Integer,by_la[:avg_payments_sys_3])
        newframe[:Incomes_Option_2b] = round.(Integer,by_la[:avg_income_sys_4])
        newframe[:Payments_Option_2b] = round.(Integer,by_la[:avg_payments_sys_4])
        newframe[:Incomes_Option_3] = round.(Integer,by_la[:avg_income_sys_5])
        newframe[:Payments_Option_3] = round.(Integer,by_la[:avg_payments_sys_5])
        newframe[:ONS_Code]=by_la[:ccode]
        newframe
    end


    function addallgrantcols!(
        natdata:: DataFrame,
        by_la :: DataFrame )
         for popcol in POPN_MEASURES
             spop = String(popcol)
             add_modelled_grants_to_la!(by_la, spop)
             add_modelled_grants_to_national!( natdata, spop )
         end
    end

    function doonerun(
        params   :: Array{Params},
        settings :: DataSettings )
        main_results =  CareData.makecareroutcomesframe(0)
        grantdata = CSV.File( DATADIR*"edited/GRANTS_2019.csv" ) |> DataFrame
        alldata = CareData.loadall()
        run_data_dir = DATADIR*"/populations/"*settings.dataset*"/"
        output_dir = RESULTSDIR*"/"*settings.name*"/"
        println( "writing output to |$output_dir|")
        mkpath( output_dir )
        numsystems = size( params )[1]
        for iteration in 1:settings.num_iterations

            yp_data = CSV.File( run_data_dir*"yp_data_$iteration.csv" ) |> DataFrame
            carer_data = CSV.File( run_data_dir*"carer_data_$iteration.csv" ) |> DataFrame

            rc = size( carer_data )[1]
            ry = size( yp_data )[1]
            @assert rc == ry
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
                    council_data = ofdata[1,:]
                    # println(mainrun_dfe ofdata.council )
                    for sysno in 1:numsystems
                        outcomes = StayingPutSim.doonecalc(
                            ccode,
                            year,
                            carer,
                            yp,
                            council_data,
                            params[sysno] )

                        CareData.addcareroutcomestoframe!(
                            main_results,
                            sysno,
                            iteration,
                            year,
                            ccode,
                            carer.id,
                            yp.age,
                            outcomes )
                    end # sys loop
                    if r % 100 == 0
                        println( carer.id )
                    end
                end # good code check
            end # main loop
        end # iteration
        CSVFiles.save( output_dir*"/main_results.csv", main_results )
        output_dir
    end # doonerun

    function addsysnotoname( names, sysno ) :: Array{Symbol,1}
        a = Array{Symbol,1}(undef, 0)
        for n in names
            if n !== :ccode && n !== :rcode && n !== :year && n !== :yp_age
                push!( a, Symbol(String( n )*"_sys_$sysno"))
            else
                push!(a, n)
            end
        end
        a
        # Symbol.(String.( names ).*"_sys_$sysno")
    end

    function createmaintables(
        output_dir :: AbstractString,
        params     :: Array{Params},
        settings   :: DataSettings  )

        num_systems = size( params )[1]
        main_results = CSVFiles.load( output_dir*"/main_results.csv" )
        by_la_sys_and_year = []
        for sysno in 1:num_systems
            by_la_sys_iteration_and_year = main_results |>
                @filter( _.year > 2018 && _.year < 2025  && _.sysno == sysno ) |>
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

            CSVFiles.save( output_dir*"by_la_sys_iteration_and_year.csv", by_la_sys_iteration_and_year, delim='\t' )

            by_la_sys_and_year_tmp = by_la_sys_iteration_and_year |>
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
            newnames = addsysnotoname( names( by_la_sys_and_year_tmp ), sysno )
            names!( by_la_sys_and_year_tmp, newnames )
            CSVFiles.save( output_dir*"by_la_sys_and_year_sysno_$sysno.csv", by_la_sys_and_year_tmp, delim='\t' )
            push!(by_la_sys_and_year, by_la_sys_and_year_tmp )
        end
        open( output_dir*"settings.txt", "w") do file
           println(file,settings)
        end
        for sysno in size(params)[1]
            open( output_dir*"params_sys_$sysno.txt", "w") do file
               println(file,params[sysno])
            end
        end
        grantdata = CSV.File( DATADIR*"edited/GRANTS_2019.csv" ) |> DataFrame
        merged = join( grantdata, by_la_sys_and_year[1], on=:ccode, makeunique=true )
        for sysno in 2:num_systems
            merged = join( merged, by_la_sys_and_year[sysno], on=[:ccode,:year], makeunique=true )
        end

        for popcol in POPN_MEASURES
            spop = String(popcol)
            add_modelled_grants_to_la!(merged, spop)
        end
        CSVFiles.save( output_dir*"by_la_sys_and_year_merged_with_grant.csv", merged, delim='\t' )
        simple = cleanup_main_frame( merged )
        CSVFiles.save( output_dir*"by_la_sys_and_year_merged_with_grant_simple_version.csv", simple, delim='\t' )
        merged
    end # createmaintables

    function createnglandtables(
        output_dir :: AbstractString,
        params     :: Array{Params},
        settings   :: DataSettings  )

        num_systems = size( params )[1]
        main_results = CSVFiles.load( output_dir*"/main_results.csv" )
        by_sys_and_year = []
        for sysno in 1:num_systems
            by_sys_iteration_and_year = main_results |>
                @filter( _.year > 2018 && _.year < 2025  && _.sysno == sysno ) |>
                @groupby( [_.year, _.sysno, _.iteration ] ) |>
                @map({
                    index=key(_),
                    year=first(_.year),
                    sysno=first(_.sysno),
                    iteration=first(_.iteration),
                    cnt=length( _ ),
                    income=sum( _.income_recieved )*52.0,
                    contribs = sum( _.contributions_from_yp )*52.0,
                    payments = sum( _.payments_from_la )*52.0
                    }

                    ) |>
                @orderby( [_.year,_.sysno, _.iteration ] ) |>
                DataFrame

            CSVFiles.save( output_dir*"england_sys_iteration_and_year.csv", by_sys_iteration_and_year, delim='\t' )

            by_sys_and_year_tmp = by_sys_iteration_and_year |>
                @groupby( [_.year, _.sysno] ) |>
                @map(
                        {
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
                @orderby( [ _.year,_.sysno] ) |>
                DataFrame
            CSVFiles.save( output_dir*"by_sys_and_year_sysno_$sysno.csv", by_sys_and_year_tmp, delim='\t' )
            push!(by_sys_and_year, by_sys_and_year_tmp )
        end
        merged = copy(by_sys_and_year[1])
        newnames = addsysnotoname( names( by_sys_and_year[1] ), 1 )
        names!( merged, newnames )

        stacked = copy( by_sys_and_year[1])

        for sysno in 2:num_systems
            stacked = vcat( stacked, copy( by_sys_and_year[sysno]))
            newnames = addsysnotoname( names( by_sys_and_year[sysno] ), sysno )
            names!( by_sys_and_year[sysno], newnames )
            merged = join( merged, by_sys_and_year[sysno], on=[:year], makeunique=true )
        end
        for popcol in POPN_MEASURES
            spop = String(popcol)
            add_modelled_grants_to_national!( merged, spop )
        end
        CSVFiles.save( output_dir*"by_sys_and_year.csv", merged, delim='\t' )
        CSVFiles.save( output_dir*"by_sys_and_year_stacked.csv", stacked, delim='\t' )
        merged
    end #

    function createnglandtablesbyage(
        output_dir :: AbstractString,
        params     :: Array{Params},
        settings   :: DataSettings  )

        num_systems = size( params )[1]
        main_results = CSVFiles.load( output_dir*"/main_results.csv" )
        by_sys_yp_age_and_year = []
        for sysno in 1:num_systems
            by_sys_yp_age_iteration_and_year = main_results |>
                @filter( _.year > 2018 && _.year < 2025  && _.sysno == sysno ) |>
                @groupby( [_.year, _.sysno, _.yp_age, _.iteration ] ) |>
                @map({
                    index=key(_),
                    year=first(_.year),
                    sysno=first(_.sysno),
                    yp_age = first(_.yp_age),
                    iteration=first(_.iteration),
                    cnt=length( _ ),
                    income=sum( _.income_recieved )*52.0,
                    contribs = sum( _.contributions_from_yp )*52.0,
                    payments = sum( _.payments_from_la )*52.0
                    }

                    ) |>
                @orderby( [_.year,_.sysno, _.yp_age, _.iteration ] ) |>
                DataFrame

            CSVFiles.save( output_dir*"by_sys_yp_age_iteration_and_year.csv", by_sys_yp_age_iteration_and_year, delim='\t' )
                                                                            # by_sys_yp_age_iteration_and_year

            by_sys_yp_age_and_year_tmp = by_sys_yp_age_iteration_and_year |>
                @groupby( [_.year, _.sysno, _.yp_age] ) |>
                @map(
                        {
                           year   = first(_.year),
                           sysno  = first(_.sysno),
                           yp_age  = first(_.yp_age),

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
                @orderby( [ _.year, _.yp_age, _.sysno] ) |>
                DataFrame
            CSVFiles.save( output_dir*"by_sys_yp_age_and_year_sysno_$sysno.csv", by_sys_yp_age_and_year_tmp, delim='\t' )
            push!(by_sys_yp_age_and_year, by_sys_yp_age_and_year_tmp )
        end

        merged = copy(by_sys_yp_age_and_year[1])
        newnames = addsysnotoname( names( merged ), 1 )
        names!(merged, newnames )
        stacked = copy(by_sys_yp_age_and_year[1])
        for sysno in 2:num_systems
            stacked = vcat( stacked, copy(by_sys_yp_age_and_year[sysno] ))
            newnames = addsysnotoname( names( by_sys_yp_age_and_year[sysno] ), sysno )
            names!( by_sys_yp_age_and_year[sysno], newnames )
            merged = join( merged, by_sys_yp_age_and_year[sysno], on=[:year,:yp_age], makeunique=true )
        end
        CSVFiles.save( output_dir*"by_sys_yp_age_and_year.csv", merged, delim='\t' )
        stacked[:system_name] = mapname( stacked[:sysno], params )
        CSVFiles.save( output_dir*"by_sys_yp_age_and_year_stacked.csv", stacked, delim='\t' )
        merged
    end #

    function mapname( sysno :: Vector, params :: Array{Params} ) :: Array{AbstractString}
        n = size( sysno )[1]
        out = Array{AbstractString}( undef, n )
        for i in 1:n
            out[i] = params[sysno[i]].name
        end
        out
    end

    function createmaintablesgrant( latotals, annualtotals)
        @assert isiterabletable( latotals )
        @assert isiterabletable( annualtotals )

        for p in POPN_MEASURES
            popsharela = latotals[p] ./ sum( latotals[p] )
        end
    end
end # module
