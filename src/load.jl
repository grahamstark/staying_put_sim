module load

    using DataCreationDriver
    using StayingPutModelDriver
    using CareData
    using FosterParameters
    using GlobalDecls
    using Logging

    function do_reform_runs(
        ;
        runname   :: AbstractString,
        whichdata :: AbstractString,
        numiter   :: Integer,
        year      :: Integer,
        create_data :: Bool )

        settings = CareData.default_data_settings()
        settings.dataset = whichdata
        settings.num_iterations = numiter
        settings.name = runname
        ### FIXME REMOVE THIS
        settings.targets = ["E06000047","E0600054"]

        params = Array{Params}(undef,0)
        params1 = FosterParameters.get_default_params()
        params1.name = "Current System"
        # sys 1
        push!( params, params1 )

        params2a1 = FosterParameters.get_default_params()
        params2a1.yp_contrib_type = no_contribution
        params2a1.contrib_hb = no_contribution
        params2a1.name = "option 2(a) with Oldham style fee"
        params2a1.payment = min_payment
        params2a1.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham FIXME UPRATE
        # sys 2
        push!( params, params2a1 )

        params2a2 = FosterParameters.get_default_params()
        params2a2.yp_contrib_type = no_contribution
        params2a2.contrib_hb = no_contribution
        params2a2.name = "option 2(a) without fee"
        params2a2.payment = min_payment
        params2a2.fee = []
        # sys 3
        push!( params, params2a2 )

        params2b = FosterParameters.get_default_params()
        params2b.name = "option 2(b) with HB from all benefit recipients"
        params2b.payment = min_payment
        params2b.yp_contrib_type = no_contribution
        params2b.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        params2b.contrib_hb = benefits_only
        # sys 4
        push!( params, params2b )

        params3 = FosterParameters.get_default_params()
        params3.yp_contrib_type = no_contribution
        params3.contrib_hb = no_contribution
        params3.name = "option 3 - 2(a) with taper"
        params3.payment = min_payment
        params3.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        params3.taper = [1.0, 0.5, 0.25]
        # sys 5
        push!( params, params3 )

        params4 = FosterParameters.get_default_params()
        params4.yp_contrib_type = no_contribution
        params4.contrib_hb = all_people
        params4.name = "option 4: Oldham style fee and contributions via HB from all."
        params4.payment = min_payment
        params4.fee = [0, 78.38, 158.76, 237.23, 340.76]
        # sys 6
        push!( params, params4 )

        params5 = FosterParameters.get_default_params()
        params5.yp_contrib_type = hb_only
        params5.contrib_hb = benefits_only
        params5.name = "option 5: option 2(a) with Oldham style fee only for those with no u18 children."
        params5.payment = min_payment
        params5.fee = [0, 78.38, 158.76, 237.23, 340.76]
        params5.prop_fees_deleted = 0.84 # see email Vicki Swain 09Jan
        # sys 7
        push!( params, params5 )

        if create_data
            DataCreationDriver.create_data( settings )
        end
        nparams = length( params )[1]
        outdir = StayingPutModelDriver.do_one_run( params, settings, year )
        StayingPutModelDriver.dump_run_info( outdir, params, settings )
        StayingPutModelDriver.create_main_tables( outdir, nparams, year )
        StayingPutModelDriver.create_england_tables( outdir, params, settings, year )
        StayingPutModelDriver.create_england_tables_by_age( outdir, params, settings, year )
        StayingPutModelDriver.create_main_tables_by_region( outdir, nparams, year )
    end

    """
    FIXME this is a HORRIBLE hack
    see the FIXMEs in StayingPutModelDriver
    """
    function create_region_tables(
        ;
        runname   :: AbstractString,
        whichdata :: AbstractString,
        numiter   :: Integer,
        year      :: Integer )
        nparams = 5
        outdir = "$(RESULTSDIR)/main-results-$(whichdata)/"
        StayingPutModelDriver.create_main_tables_by_region( outdir, nparams, year )
    end

    function create_data( ds :: DataPublisher, pc :: Real, numiter :: Integer, createdata :: Bool, year :: Integer )
        settings = CareData.default_data_settings()
        settings.name = "using-$ds-$pc-pct"
        settings.dataset = "ds-$ds-$pc-pct"
        settings.annual_sp_increment = pc # 1% inrease in all SP rates per year
        settings.description = "Main run, with $pc annual increase in rates of retention data $ds; iterations $numiter"
        settings.num_iterations = numiter
        settings.reaching_18s_source = ds
        DataCreationDriver.create_data( settings )
    end

    function do_main_run( ds :: DataPublisher, pc :: Real, numiter :: Integer, createdata :: Bool, year :: Integer )
        settings = CareData.default_data_settings()
        settings.name = "using-$ds-$pc-pct"
        settings.dataset = "ds-$ds-$pc-pct"
        settings.annual_sp_increment = pc # 1% inrease in all SP rates per year
        settings.description = "Main run, with $pc annual increase in rates of retention data $ds; iterations $numiter"
        settings.num_iterations = numiter
        settings.reaching_18s_source = ds

        if createdata
            DataCreationDriver.create_data( settings )
        end
        params = Array{Params}(undef,0)
        params1 = FosterParameters.get_default_params()

        push!( params, params1 )

        params2 = FosterParameters.get_default_params()
        params2.payment = min_payment
        push!( params, params2 )

        params3 = FosterParameters.get_default_params()
        params3.yp_contrib_type = no_contribution
        push!( params, params3 )

        num_systems = size( params )[1]

        outdir = StayingPutModelDriver.do_one_run( params, settings, year )
        StayingPutModelDriver.create_main_tables( outdir, num_systems, :rcode, year )
        StayingPutModelDriver.create_main_tables( outdir, num_systems, :ccode, year )
        StayingPutModelDriver.create_england_tables( outdir, params, settings, year )
        StayingPutModelDriver.create_england_tablesbyage( outdir, params, settings, year )
    end #  function

    numiter = 200
    do_create_data = false
    year = 2020
    #do_main_run( OFSTED, 0.0, numiter, false, year )
    #do_main_run( DFE, 0.0, numiter, false, year )
    #do_main_run( OFSTED, 0.01, numiter, true, year )
    #do_main_run( DFE, 0.01, numiter, true, year )

    # create_data( DFE, 0.0, numiter, false, year )
    # create_data( OFSTED, 0.01, numiter, true, year )
    # create_data( DFE, 0.01, numiter, true, year )

    # configure logger; see: https://docs.julialang.org/en/v1/stdlib/Logging/index.html
    # and: https://github.com/oxinabox/LoggingExtras.jl
    logger = FileLogger("/home/graham_s/tmp/afc_log.txt")
    global_logger(logger)
    LogLevel( Logging.Info )

    DEFAULT_DATA = "ds-DFE-0.0-pct"

    datasets = [
        "ds-DFE-0.01-pct",
        "ds-DFE-0.0-pct",
        "ds-OFSTED-0.01-pct",
        "ds-OFSTED-0.0-pct"
    ]

    #dsname = datasets[1]
    #do_reform_runs(
    #     runname   = "test1",
    #     whichdata = DEFAULT_DATA,
#         numiter   = 1,
#         year      = 2020 )
    #
if size(ARGS)[1] > 0
    numiter = parse( Int, ARGS[1])
end

if size(ARGS)[1] > 1
    do_create_data = parse( Int, ARGS[2]) == 1
end

for dsname in datasets
    if dsname == "ds-DFE-0.0-pct"
       do_reform_runs(
            runname   = "main-results-"*dsname,
            whichdata = dsname,
            numiter   = numiter,
            year      = 2020,
            create_data = do_create_data )
    end
end

#create_region_tables(
        #    runname   = "main-results-$(DEFAULT_DATA)",
        #    whichdata = DEFAULT_DATA,
        #    numiter   = 1,
    #        year      = 2020 )

#for dsname in datasets
#        create_region_tables(
#            runname   = "main-results-"*dsname,
#            whichdata = dsname,
#            numiter   = 200 )
#end

    # pdir = "/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/2020/"
    # outdir = pdir*"/test1/"
    # num_systems = 5
    # StayingPutModelDriver.create_main_tables( outdir, num_systems, :ccode )
    # StayingPutModelDriver.create_main_tables( outdir, num_systems, :rcode )
end
