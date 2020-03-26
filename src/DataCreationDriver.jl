module DataCreationDriver

    using CSVFiles
    using Base.Filesystem

    using CareData
    using GlobalDecls

    export createdata

    #settings.reaching_18s_source = DFE
    #settings.num_iterations = 200
    # push!(settings.targets, "E10000030" )
    #settings.agglevel = national

    function create_data( settings :: DataSettings )
        run_data_dir = DATADIR*"/populations/$(settings.datayear)/$(settings.dataset)/"
        mkpath( run_data_dir )
        alldata = CareData.load_all( settings.datayear )
        for k in 1:settings.num_iterations
            println( "creating iteration $k")
            created = CareData.create_base_datasets(
                alldata.ofdata,
                settings );
            CSVFiles.save( run_data_dir*"/yp_data_$k.csv", created.yp_data )
            CSVFiles.save( run_data_dir*"/carer_data_$k.csv", created.carer_data )
        end
    end # create data

end # module
