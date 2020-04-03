module load

    using DataCreationDriver
    using StayingPutModelDriver
    using CareData
    using FosterParameters
    using GlobalDecls

    function doreformruns(
        ;
        runname   :: AbstractString,
        whichdata :: AbstractString,
        numiter   :: Integer,
        year      :: Integer )

        settings = CareData.default_data_settings()
        settings.dataset = whichdata
        settings.num_iterations = numiter
        settings.name = runname

        params = Array{Params}(undef,0)
        params1 = FosterParameters.get_default_params()
        params1.name = "Current System"

        push!( params, params1 )

        params2a1 = FosterParameters.get_default_params()
        params2a1.yp_contrib_type = no_contriWbution
        params2a1.contrib_hb = no_contribution
        params2a1.name = "option 2(a) with Oldham style fee"
        params2a1.payment = min_payment
        params2a1.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        push!( params, params2a1 )

        params2a2 = FosterParameters.get_default_params()
        params2a2.yp_contrib_type = no_contribution
        params2a2.contrib_hb = no_contribution
        params2a2.name = "option 2(a) without fee"
        params2a2.payment = min_payment
        params2a2.fee = []
        push!( params, params2a2 )

        params2b = FosterParameters.get_default_params()
        params2b.name = "option 2(b) with HB from all benefit recipients"
        params2b.payment = min_payment
        params2b.yp_contrib_type = no_contribution
        params2b.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        params2b.contrib_hb = benefits_only
        push!( params, params2b )

        params3 = FosterParameters.get_default_params()
        params3.yp_contrib_type = no_contribution
        params3.contrib_hb = no_contribution
        params3.name = "option 3 - 2(a) with taper"
        params3.payment = min_payment
        params3.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        params3.taper = [1.0, 0.5, 0.25]
        push!( params, params3 )

        nparams = length( params )[1]
        outdir = StayingPutModelDriver.doonerun( params, settings, year )
        StayingPutModelDriver.dumpruninfo( outdir, params, settings )
        StayingPutModelDriver.createmaintables( outdir, nparams, :ccode, year )
        StayingPutModelDriver.createmaintables( outdir, nparams, :rcode, year )
        StayingPutModelDriver.createnglandtables( outdir, params, settings, year )
        StayingPutModelDriver.createnglandtablesbyage( outdir, params, settings, year )
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
        StayingPutModelDriver.createmaintables_by_region( outdir, nparams )
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

        outdir = StayingPutModelDriver.doonerun( params, settings, year )
        StayingPutModelDriver.createmaintables( outdir, num_systems, :rcode, year )
        StayingPutModelDriver.createmaintables( outdir, num_systems, :ccode, year )
        StayingPutModelDriver.createnglandtables( outdir, params, settings, year )
        StayingPutModelDriver.createnglandtablesbyage( outdir, params, settings, year )
    end #  function

    numiter = 200
    year = 2020
    do_main_run( OFSTED, 0.0, numiter, false, year )
    do_main_run( DFE, 0.0, numiter, false, year )
    do_main_run( OFSTED, 0.01, numiter, true, year )
    do_main_run( DFE, 0.01, numiter, true, year )

    DEFAULT_DATA = "ds-DFE-0.01-pct"

    datasets = [
        "ds-DFE-0.01-pct",
        "ds-DFE-0.0-pct",
        "ds-OFSTED-0.01-pct",
        "ds-OFSTED-0.0-pct"
    ]

    # dsname = datasets[1]
    # doreformruns(
    #     runname   = "test1",
    #     whichdata = DEFAULT_DATA,
    #     numiter   = 1 )
    # return
    #
#    for dsname in datasets
#        doreformruns(
#            runname   = "main-results-"*dsname,
#            whichdata = dsname,
#            numiter   = 200 )
#    end
create_region_tables(
            runname   = "main-results-$(DEFAULT_DATA)",
            whichdata = DEFAULT_DATA,
            numiter   = 200 )

#for dsname in datasets
#        create_region_tables(
#            runname   = "main-results-"*dsname,
#            whichdata = dsname,
#            numiter   = 200 )
#end

    pdir = "/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/2020/"
    # outdir = pdir*"/test1/"
    # num_systems = 5
    # StayingPutModelDriver.createmaintables( outdir, num_systems, :ccode )
    # StayingPutModelDriver.createmaintables( outdir, num_systems, :rcode )
end
