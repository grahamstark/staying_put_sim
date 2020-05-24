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
    using FosterParameters

    export do_one_run, create_main_tables, create_england_tables_by_age
    export dump_run_info, add_all_grant_cols!, cleanup_main_frame, merge_grants_to_regions

    POPN_MEASURES = [
        :avg_cnt_sys_1,:min_cnt_sys_1,:max_cnt_sys_1,
        :pct_10_cnt_sys_1,:pct_25_cnt_sys_1,:pct_75_cnt_sys_1,
        :pct_90_cnt_sys_1]

    function add_modelled_grants_to_national!(
        natdata :: DataFrame,
        which_population_measure :: AbstractString,
        start_year::Integer )
        new_population_column = Symbol( which_population_measure )
        grantcol = Symbol( which_population_measure * "_grant")
        grantdata = CSV.File( DATADIR*"edited/$(start_year)/GRANTS_BY_REGION_$(start_year).csv" ) |> DataFrame
        grant = sum( grantdata[!,:amount])
        # natdata[1,:].amount
        # grant = uprate( grant, 2019, AFC_SURVEY_YEAR ).*1.02 # hacked 1 year inflation, 1 year growth
        popn = natdata[!,new_population_column]
        natdata[!,grantcol] = track_series(
            grant,#    :: Real,
            popn ) #  :: Vector,
    end

    """
    this adds a column of grants to the data, where the grants track one or other
    of our generated counts of populations (minimum/maximum/average modelled population - see the
    constant array POPN_MEASURES above for a lost)
    """
    function add_modelled_grants_to_la!( by_la :: DataFrame, indices_for_this_la_population_measure :: AbstractString )
        new_population_column = Symbol( indices_for_this_la_population_measure )
        grantcol = Symbol( "$(indices_for_this_la_population_measure)_grant")
        nrows = size( by_la )[1]
        by_la[!,grantcol] .= 0.0
        ccodes = unique( by_la.ccode ) # all the lates
        i = 1
        for ccode in ccodes
            indices_for_this_la = (by_la.ccode .== ccode)
            one_la_all_periods = by_la[indices_for_this_la,:] # all the rows for 1 LA
            lasize = size( one_la_all_periods )[1]
            grant = one_la_all_periods[1,:].amount # this is the actual grant, for the 1st period (2020,2019 or whenever)
            popn = one_la_all_periods[!,new_population_column] # we're expanding the grant to track this popn measure
            tracked_grant = track_series(
                grant,#    :: Real,
                popn ) #  :: Vector,
            j = i+lasize-1
            by_la[i:j,grantcol] = tracked_grant
            i += lasize
        end
    end

    function add_modelled_grants_to_region!( by_region :: DataFrame, which_population_measure :: AbstractString )
        new_population_column = Symbol( which_population_measure )
        grantcol = Symbol( "$(new_population_column)_grant")
        nrows = size( by_region )[1]
        by_region[!,grantcol] .= 0.0
        rcodes = unique( by_region.rcode )
        i = 1
        for rcode in rcodes # FIXME could actually be arcode
            which = (by_region.rcode .== rcode)
            later = by_region[which,:]
            rsize = size( later )[1]
            grant = later[1,:].amount
            # grant = uprate( grant, 2019, AFC_SURVEY_YEAR ).*1.02 # hacked 1 year inflation, 1 year growth
            popn = later[!,new_population_column]
            tracked_grant = track_series(
                grant,#    :: Real,
                popn ) #  :: Vector,
                #base_period :: Integer = 1,
                #base_year   :: Integer = AFC_SURVEY_YEAR
            # println( "rcode=$rcode tracked_grant=$tracked_grant")
            j = i+rsize-1
            by_region[i:j,grantcol] = tracked_grant
            i += rsize
        end
    end

    function cleanup_main_frame( by_la :: DataFrame )
        newframe=DataFrame()
        print( names( by_la ))
        newframe[!,:Year]=by_la[!,:year]
        newframe[!,:Council]=by_la[!,:council]
        newframe[!,:Number]=round.(Integer,by_la[!,:avg_cnt_sys_1])
        newframe[!,:Grants_Option_1] = round.(Integer,by_la[!,:avg_cnt_sys_1_grant])
        newframe[!,:Incomes_Option_1] = round.(Integer,by_la[!,:avg_income_sys_1])
        newframe[!,:Payment_Option_1] = round.(Integer,by_la[!,:avg_payments_sys_1])
        newframe[!,:Incomes_Option_2a_1] = round.(Integer,by_la[!,:avg_income_sys_2])
        newframe[!,:Payments_Option_2a_1] = round.(Integer,by_la[!,:avg_payments_sys_2])
        newframe[!,:Incomes_Option_2a_2] = round.(Integer,by_la[!,:avg_income_sys_3])
        newframe[!,:Payments_Option_2a_2] = round.(Integer,by_la[!,:avg_payments_sys_3])
        newframe[!,:Incomes_Option_2b] = round.(Integer,by_la[!,:avg_income_sys_4])
        newframe[!,:Payments_Option_2b] = round.(Integer,by_la[!,:avg_payments_sys_4])
        newframe[!,:Incomes_Option_3] = round.(Integer,by_la[!,:avg_income_sys_5])
        newframe[!,:Payments_Option_3] = round.(Integer,by_la[!,:avg_payments_sys_5])
        newframe[!,:Incomes_Option_4] = round.(Integer,by_la[!,:avg_income_sys_6])
        newframe[!,:Payments_Option_4] = round.(Integer,by_la[!,:avg_payments_sys_6])
        newframe[!,:Incomes_Option_5] = round.(Integer,by_la[!,:avg_income_sys_7])
        newframe[!,:Payments_Option_5] = round.(Integer,by_la[!,:avg_payments_sys_7])
        newframe[!,:ONS_Code]=by_la[!,:ccode]
        newframe
    end

    function cleanup_main_frame_regional( by_la :: DataFrame )
        newframe=DataFrame()
        newframe[!,:Year]=by_la[!,:year]
        # if( target == :ccode )
        #     newframe[!,:Council]=by_la[!,:council]
        # end
        newframe[!,:Number]=round.(Integer,by_la[!,:avg_cnt_sys_1])
        newframe[!,:Grants_Option_1] = round.(Integer,by_la[!,:avg_cnt_sys_1_grant])
        newframe[!,:Incomes_Option_1] = round.(Integer,by_la[!,:avg_income_sys_1])
        newframe[!,:Incomes_Option_2a_1] = round.(Integer,by_la[!,:avg_income_sys_2])
        newframe[!,:Payments_Option_2a_1] = round.(Integer,by_la[!,:avg_payments_sys_2])
        newframe[!,:Incomes_Option_2a_2] = round.(Integer,by_la[!,:avg_income_sys_3])
        newframe[!,:Payments_Option_2a_2] = round.(Integer,by_la[!,:avg_payments_sys_3])
        newframe[!,:Incomes_Option_2b] = round.(Integer,by_la[!,:avg_income_sys_4])
        newframe[!,:Payments_Option_2b] = round.(Integer,by_la[!,:avg_payments_sys_4])
        newframe[!,:Incomes_Option_3] = round.(Integer,by_la[!,:avg_income_sys_5])
        newframe[!,:Payments_Option_3] = round.(Integer,by_la[!,:avg_payments_sys_5])
        newframe[!,:Incomes_Option_4] = round.(Integer,by_la[!,:avg_income_sys_6])
        newframe[!,:Payments_Option_4] = round.(Integer,by_la[!,:avg_payments_sys_6])
        newframe[!,:ONS_Code]=by_la[!,:rcode]
        newframe
    end



    function add_all_grant_cols!(
        natdata:: DataFrame,
        by_la :: DataFrame,
        start_year :: Integer )
         for new_population_column in POPN_MEASURES
             spop = String(new_population_column)
             add_modelled_grants_to_la!(by_la, spop,start_year )
             add_modelled_grants_to_national!( natdata, spop, start_year )
         end
    end

    function do_one_run(
        params   :: Array{Params},
        settings :: DataSettings,
        year     :: Integer )
        main_results =  CareData.makecareroutcomesframe(0)
        # grantdata = CSV.File( "$(DATADIR)/edited/$(year)/GRANTS_$(year).csv" ) |> DataFrame
        alldata = CareData.load_all(year)
        run_data_dir = "$(DATADIR)/populations/$(year)/$(settings.dataset)/"
        output_dir = "$(RESULTSDIR)/$(year)/$(settings.name)/"
        println( "writing output to |$output_dir|")
        mkpath( output_dir )
        numsystems = size( params )[1]
        for iteration in 1:settings.num_iterations

            yp_data = CSV.File( "$(run_data_dir)/yp_data_$iteration.csv" ) |> DataFrame
            carer_data = CSV.File( "$(run_data_dir)/carer_data_$iteration.csv" ) |> DataFrame

            rc = size( carer_data )[1]
            ry = size( yp_data )[1]
            @assert rc == ry
            @time for r in 1:rc
                year = yp_data[r,:year]
                ccode = yp_data[r,:ccode]
                println( "computing for council with ccode $ccode")
                rcode = region_code_from_ccode( ccode ) # yp_data[r,:rcode]
                # this test isn't strictly needed since we only create for live councils
                if (! ONSCodes.is_aggregate( ccode )) && (! ( ccode in SKIPLIST ))
                    carer = CareData.carerfromrow( carer_data[r,:] )
                    yp = CareData.ypfromrow( yp_data[r,:])
                    @assert carer.id == yp_data[r,:carer]
                    @assert year == carer_data[r,:year]
                    which = alldata.ofdata.ccode .== ccode
                    println( "which = $which")
                    ofdata = alldata.ofdata[which,:]
                    @assert size(ofdata)[1] == 1
                    council_data = ofdata[1,:]
                    # println(mainrun_dfe ofdata.council )
                    for sysno in 1:numsystems
                        outcomes = StayingPutSim.do_one_calc(
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
                            rcode,
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
    end # do_one_run

    function addsysnotoname( names, sysno ) :: Array{Symbol,1}
        a = Array{Symbol,1}(undef, 0)
        for n in names
            if n !== :ccode && n !== :rcode && n !== :year && n !== :yp_age && n !== :targetcode
                push!( a, Symbol(String( n )*"_sys_$sysno"))
            else
                push!(a, n)
            end
        end
        a
        # Symbol.(String.( names ).*"_sys_$sysno")
    end


    function getposoftarget( main_results :: DataFrame, target :: Symbol ) :: Integer
        ns = names( main_results )
        lns = length(ns)[1]
        targetpos = -1
        for i in 1:lns
            if ns[i] == target
                targetpos = i
                break
            end
        end
        targetpos
    end

    function dump_run_info(
        output_dir :: AbstractString,
        params     :: Array{Params},
        settings   :: DataSettings )
        open( output_dir*"settings.txt", "w") do file
            println( file, settings )
        end
        for sysno in size(params)[1]
            open( output_dir*"params_sys_$sysno.txt", "w") do file
               println(file,params[sysno])
            end
        end
    end

    """
     FIXME FIXME FIXME attempt to pass in grouping target as a parameter,
     so I can add groupings by rcode as well as council
     has broken this in ways I don't understand. See this line, probably:

     targetcode=first( _[1][targetpos])
    """
    function create_main_tables(
        output_dir  :: AbstractString,
        num_systems :: Integer,
        start_year :: Integer )
        # whichgroup :: Symbol
        all_base_data = CareData.load_all(2020)
        main_results = CSV.File( output_dir*"/main_results.csv" ) |> DataFrame # don't really need the cast
        by_council_sys_and_year = []
        println( "create_main_tables; target is ccol")
        for sysno in 1:num_systems
            by_council_sys_iteration_and_year = main_results |>
                @filter( _.year >= start_year && _.year <= 2026  && _.sysno == sysno ) |>
                @groupby( [ _.ccode,  _.year, _.sysno, _.iteration] ) |>
                @map({ index=key(_),
                    year=first(_.year),
                    ccode=first( _.ccode ),
                    sysno=first(_.sysno),
                    iteration=first(_.iteration),
                    cnt=length( _ ),
                    income=sum( _.income_recieved )*52.0,
                    contribs = sum( _.contributions_from_yp )*52.0,
                    payments = sum( _.payments_from_la )*52.0
                    } ) |>
                @orderby( [_.year,_.ccode,_.sysno, _.iteration ] ) |>
                DataFrame

            CSVFiles.save( output_dir*"by_council_sys_iteration_and_year.csv", by_council_sys_iteration_and_year, delim='\t' )
            # the sorting things fail because of some

            by_council_sys_and_year_tmp = by_council_sys_iteration_and_year |>
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
            newnames = addsysnotoname( names( by_council_sys_and_year_tmp ), sysno )
            rename!( by_council_sys_and_year_tmp, newnames )
            CSVFiles.save( output_dir*"by_ccode_sys_and_year_sysno_$sysno.csv", by_council_sys_and_year_tmp, delim='\t' )
            push!(by_council_sys_and_year, by_council_sys_and_year_tmp )
        end

        # grantdata = CSV.File( DATADIR*"edited/GRANTS_2019.csv" ) |> DataFrame
        grantdata = all_base_data.grantdata
        merged = join( grantdata, by_council_sys_and_year[1], on=:ccode, makeunique=true )
        for sysno in 2:num_systems
            merged = join( merged, by_council_sys_and_year[sysno], on=[:ccode,:year], makeunique=true )
        end

        for new_population_column in POPN_MEASURES
            spop = String(new_population_column)
            add_modelled_grants_to_la!(merged, spop )
        end
        CSVFiles.save( output_dir*"by_ccode_sys_and_year_merged_with_grant.csv", merged, delim='\t' )
        simple = cleanup_main_frame( merged )
        CSVFiles.save( output_dir*"by_ccode_sys_and_year_merged_with_grant_simple_version.csv", simple, delim='\t' )
        merged
    end # create_main_tables


    """
     FIXME FIXME FIXME FIXME should be no need for this!!!
     create_main_tables above should do everything but has problems so I'm
     just hacking this near-duplicate
    """
    function create_main_tables_by_region(
        output_dir :: AbstractString,
        num_systems :: Integer,
        start_year :: Integer )

        main_results = CSVFiles.load( output_dir*"/main_results.csv" ) |> DataFrame
        by_region_sys_and_year = []

        main_results[!,:rcode] = map( ccode->region_code_from_ccode( ccode ), main_results[!,:ccode ] )
        # data[rcode]=map( c->region_code_from_name(c), data[cname])
        for sysno in 1:num_systems
            by_region_sys_iteration_and_year = main_results |>
                @filter( _.year >= start_year && _.year <= 2026  && _.sysno == sysno ) |>
                @groupby( [_.rcode,  _.year, _.sysno, _.iteration ] ) |>
                @map({
                    index=key(_),
                    year=first(_.year),
                    rcode=first(_.rcode),
                    sysno=first(_.sysno),
                    iteration=first(_.iteration),
                    cnt=length( _ ),
                    income=sum( _.income_recieved )*52.0,
                    contribs = sum( _.contributions_from_yp )*52.0,
                    payments = sum( _.payments_from_la )*52.0
                    }

                    ) |>
                @orderby( [_.year,_.rcode,_.sysno, _.iteration ] ) |>
                DataFrame

            CSVFiles.save( output_dir*"by_region_sys_iteration_and_year.csv", by_region_sys_iteration_and_year, delim='\t' )

            by_region_sys_and_year_tmp = by_region_sys_iteration_and_year |>
                @groupby( [_.rcode,  _.year, _.sysno] ) |>
                @map(
                        {
                           rcode  = first(_.rcode),
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
                @orderby( [ _.rcode, _.year,_.sysno] ) |>
                DataFrame
            newnames = addsysnotoname( names( by_region_sys_and_year_tmp ), sysno )
            rename!( by_region_sys_and_year_tmp, newnames )
            CSVFiles.save( output_dir*"by_region_sys_and_year_sysno_$sysno.csv", by_region_sys_and_year_tmp, delim='\t' )
            push!(by_region_sys_and_year, by_region_sys_and_year_tmp )
        end
        grantdata = CSV.File( DATADIR*"edited/$(start_year)/GRANTS_BY_REGION_$(start_year).csv" ) |> DataFrame
        merged = join( grantdata, by_region_sys_and_year[1], on=:rcode, makeunique=true )
        for sysno in 2:num_systems
            merged = join( merged, by_region_sys_and_year[sysno], on=[:rcode,:year], makeunique=true )
        end

        for new_population_column in POPN_MEASURES
            spop = String(new_population_column)
            add_modelled_grants_to_region!(merged, spop)
        end
        CSVFiles.save( output_dir*"by_region_sys_and_year_merged_with_grant.csv", merged, delim='\t' )
        simple = cleanup_main_frame_regional( merged )
        CSVFiles.save( output_dir*"by_region_sys_and_year_merged_with_grant_simple_version.csv", simple, delim='\t' )
        merged
    end # create_main_tables_by_region


    function merge_grants_to_regions( grantdata :: DataFrame ) :: DataFrame
        return  grantdata |>
                @filter( _.amount > 0 ) |>
                @groupby( [_.rcode ] ) |>
                @map(
                        {
                           targetcode = first(_.rcode ),
                           amount=sum( _.amount  )
                        } ) |>
                @orderby( _.targetcode ) |>
                DataFrame
    end

    function create_england_tables(
        output_dir :: AbstractString,
        params     :: Array{Params},
        settings   :: DataSettings,
        start_year :: Integer )

        num_systems = size( params )[1]
        main_results = CSVFiles.load( output_dir*"/main_results.csv" )
        by_sys_and_year = []
        for sysno in 1:num_systems
            by_sys_iteration_and_year = main_results |>
                @filter( _.year >= start_year && _.year <= 2026  && _.sysno == sysno ) |>
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
        rename!( merged, newnames )

        stacked = copy( by_sys_and_year[1])

        for sysno in 2:num_systems
            stacked = vcat( stacked, copy( by_sys_and_year[sysno]))
            newnames = addsysnotoname( names( by_sys_and_year[sysno] ), sysno )
            rename!( by_sys_and_year[sysno], newnames )
            merged = join( merged, by_sys_and_year[sysno], on=[:year], makeunique=true )
        end
        for new_population_column in POPN_MEASURES
            spop = String(new_population_column)
            add_modelled_grants_to_national!( merged, spop, start_year )
        end
        CSVFiles.save( output_dir*"by_sys_and_year.csv", merged, delim='\t' )
        CSVFiles.save( output_dir*"by_sys_and_year_stacked.csv", stacked, delim='\t' )
        merged
    end # create_england_tables

    function create_england_tables_by_age(
        output_dir :: AbstractString,
        params     :: Array{Params},
        settings   :: DataSettings,
        start_year :: Integer  )

        num_systems = size( params )[1]
        main_results = CSVFiles.load( output_dir*"/main_results.csv" )
        by_sys_yp_age_and_year = []
        for sysno in 1:num_systems
            by_sys_yp_age_iteration_and_year = main_results |>
                @filter( _.year >= start_year && _.year <= 2026  && _.sysno == sysno ) |>
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
        rename!(merged, newnames )
        stacked = copy(by_sys_yp_age_and_year[1])
        for sysno in 2:num_systems
            stacked = vcat( stacked, copy(by_sys_yp_age_and_year[sysno] ))
            newnames = addsysnotoname( names( by_sys_yp_age_and_year[sysno] ), sysno )
            rename!( by_sys_yp_age_and_year[sysno], newnames )
            merged = join( merged, by_sys_yp_age_and_year[sysno], on=[:year,:yp_age], makeunique=true )
        end
        CSVFiles.save( output_dir*"by_sys_yp_age_and_year.csv", merged, delim='\t' )
        stacked[!,:system_name] = mapname( stacked[!, :sysno], params )
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

    function create_main_tablesgrant( latotals, annualtotals)
        @assert isiterabletable( latotals )
        @assert isiterabletable( annualtotals )

        for p in POPN_MEASURES
            popsharela = latotals[p] ./ sum( latotals[p] )
        end
    end
end # module
