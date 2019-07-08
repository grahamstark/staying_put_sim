module load

    using DataCreationDriver
    using StayingPutModelDriver
    using CareData
    using Parameters

    function doreformruns(
        ;
        runname   :: AbstractString,
        whichdata :: AbstractString,
        numiter   :: Integer )

        settings = CareData.default_data_settings()
        settings.dataset = whichdata
        settings.num_iterations = numiter
        settings.name = runname

        params = Array{Params}(undef,0)
        params1 = Parameters.getdefaultparams()
        push!( params, params1 )

        params2a1 = Parameters.getdefaultparams()
        params2a1.yp_contrib_type = no_contribution
        params2a1.contrib_hb = no_contribution
        params2a1.name = "option 2(a), with Oldham style fee"
        params2a1.payment = min_payment
        params2a1.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        push!( params, params2a1 )

        params2a2 = Parameters.getdefaultparams()
        params2a2.yp_contrib_type = no_contribution
        params2a2.contrib_hb = no_contribution
        params2a2.name = "option 2(a), without fee"
        params2a2.payment = min_payment
        params2a2.fee = []
        push!( params, params2a2 )

        params2b = Parameters.getdefaultparams()
        params2b.name = "option 2(b), with HB from all benefit recipients"
        params2b.payment = min_payment
        params2b.yp_contrib_type = no_contribution
        params2b.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        params2b.contrib_hb = benefits_only
        push!( params, params2b )

        params3 = Parameters.getdefaultparams()
        params3.yp_contrib_type = no_contribution
        params3.contrib_hb = no_contribution
        params3.name = "option 3 - 2(a) with taper"
        params3.payment = min_payment
        params3.fee = [0, 78.38, 158.76, 237.23, 340.76] # oldham
        params3.taper = [1.0, 0.5, 0.25]
        push!( params, params3 )

        outdir = StayingPutModelDriver.doonerun( params, settings )
        StayingPutModelDriver.createmaintables( outdir, params, settings )
        StayingPutModelDriver.createnglandtables( outdir, params, settings )
        StayingPutModelDriver.createnglandtablesbyage( outdir, params, settings )
    end

    function mainrun( ds :: DataPublisher, pc :: Real, numiter :: Integer )
        settings = CareData.default_data_settings()
        settings.name = "using-$ds-$pc-pct"
        settings.dataset = "ds-$ds-$pc-pct"
        settings.annual_sp_increment = pc # 1% inrease in all SP rates per year
        settings.description = "Main run, with $pc annual increase in rates of retention data $ds; iterations $numiter"
        settings.num_iterations = numiter
        settings.reaching_18s_source = ds
        createdata = true
        if createdata
            DataCreationDriver.createdata( settings )
        end
        params = Array{Params}(undef,0)
        params1 = Parameters.getdefaultparams()
        push!( params, params1 )

        params2 = Parameters.getdefaultparams()
        params2.payment = min_payment
        push!( params, params2 )

        params3 = Parameters.getdefaultparams()
        params3.yp.yp_contrib_type = no_contribution
        push!( params, params3 )

        outdir = StayingPutModelDriver.doonerun( params, settings )
        StayingPutModelDriver.createmaintables( outdir, params, settings )
        StayingPutModelDriver.createnglandtables( outdir, params, settings )
        StayingPutModelDriver.createnglandtablesbyage( outdir, params, settings )
    end #  function

    numiter = 200
    # mainrun( OFSTED, 0.0, numiter )
    # mainrun( OFSTED, 0.01, numiter )
    # mainrun( DFE, 0.0, numiter )
    # mainrun( DFE, 0.01, numiter )
    DEFAULT_DATA = "ds-DFE-0.01-pct"

    datasets = [
        "ds-DFE-0.01-pct",
        "ds-DFE-0.0-pct",
        "ds-OFSTED-0.01-pct",
        "ds-OFSTED-0.0-pct"
    ]

    for dsname in datasets
        doreformruns(
            runname   = "main-results-"*dsname,
            whichdata = dsname,
            numiter   = 200 )
    end
end
