module testsuite

    using Test
    using JuliaDB
    using DataFrames
    using Query
    using CSVFiles
    using Parameters
    #
    # local imports; see ~/.julia/config/startup.jl
    #
    using GlobalDecls
    using Utils
    using ONSCodes
    using DataEditing
    using LAModelData
    using StayingPutSim
    import CareData
    using CareData

    code    = "E06000036"
    name    = "Bracknell Forest"
    regionc = "E12000008"
    regionn = "South East England"

    CCODE_SUSSEX="E10000030"

    @testset "Data Edit Tests" begin

        rc = regioncodefromcode( code )
        @test rc == regionc

        rcc3 = regioncodefromname( name )
        @test rcc3 == regionc

        cc = codefromname( name )
        @test cc == code

        rcc = regioncodefromname( name  )
        @test rcc == regionc

        rcc2 = regioncodefromname( regionn  )
        @test rcc2 == regionc

        rcn = regionnamefromname( name )
        @test rcn == regionn
    end

    @testset "ladata tests" begin
        x=doexitratequery( 2017, "E09000002", 18 )
        exits = getstayingrates( "E09000003", local_authority, true )
    end

    alldata = CareData.loadall()

    carer_dataset = CareData.makecarerframe(0)
    yp_dataset = CareData.makeypframe(0)

    @testset "Population Creation" begin
        settings = CareData.default_data_settings()
        println( "settings=$settings")
        npy = 1000
        no = (2025-2019+1)*npy
        carer = CareData.Carer( -1, 40,0.0,0.1,0.2, 1,3 )
        yp = CareData.YP( 18, 1.0, 2.0, 3.0, OtherEd )
        @time for year in 2019:2025
            for cno in 1:npy
                cn = cno <= 200 ? "C1" : "C2"
                carer.id = cno
                CareData.addcarertoframe!( carer_dataset,  year, cn, carer )
                CareData.addyptoframe!( yp_dataset, year, cn, carer.id, yp )
            end
        end
        println( size(carer_dataset))
        println( "no $no")
        # println( carer_dataset )
        @assert size( carer_dataset )[1] == no
        @time for year in 2019:2025
            carers = CareData.getcarers( carer_dataset, "C1", year )
            @test size( carers )[1] == 200
            for c in carers
                yp = CareData.getyp( yp_dataset, year, c )
                if( c.id % 10 ) == 0
                    @test yp.age == 18
                end
            end
        end
        # try with JuliaDB and indexing
        tcarer_dataset = JuliaDB.table( carer_dataset, pkey=[:year,:id])
        typ_dataset = JuliaDB.table( yp_dataset, pkey=[:year,:carer])
        @time for year in 2019:2025
            carers = CareData.getcarers( tcarer_dataset, "C1", year )
            @test size( carers )[1] == 200
            for c in carers
                yp = CareData.getyp2( typ_dataset, year, c )
                if( c.id % 200 ) == 0
                    @test yp.age == 18
                end
            end
        end

        for t = 1:3
            carer_data = CareData.makecarerframe(0)
            yp_data = CareData.makeypframe(0)
            ladata = Dict()
            if t == 1
                stayingrates = [0.0,0.0,0.0]
            elseif t == 2
                stayingrates = [0.8,0.5,0.8]
            elseif t == 3
                stayingrates = [1.0,1.0,1.0]
            end
            pid = 0
            for year in 2019:2030
                for i in 1:1000
                    pid += 1
                    CareData.addageddata!(
                        yp_dataset = yp_data,
                        carer_dataset = carer_data,
                        pid=pid,
                        ccode="COUNCIL01",
                        ladata = Dict(),
                        stayingrates=stayingrates,
                        startyear=year
                    )
                end # add 1000
            end # years
            len = size( carer_data )[1]
            println( "case: $t  len ")
            if t == 3
                @assert size( carer_data )[1]==3*12000
                @assert size( yp_data )[1]==3*12000
            elseif t == 2
                @assert size( carer_data )[1]>=0
                @assert size( yp_data )[1]>=0
            elseif t == 1
                @assert size( carer_data )[1]==0
                @assert size( yp_data )[1]==0
            end
        end # t loop

    end # popn creation testsa

    @testset "Calculation tests" begin

        carer = CareData.Carer( -1, 40,0.0,0.1,0.2, 1,3 )
        yp = CareData.YP( 18, 1.0, 2.0, 3.0, OtherEd )

        cq = CareData.loadall()
        council = cq.ofdata[50,:]
        println(typeof( council ))
        println( council.rcode )
        outcomes = CarerOutcomes( 100.0, 50.0, 25.0, 150.0 )
        baseoutcomes = CarerOutcomes( 100.0, 50.0, 25.0, 150.0 )
        params = getdefaultparams()
        settings = default_data_settings()
        year = 2019
        StayingPutSim.overrideoutcomes!(
            outcomes,
            council.ccode,
            year,#         :: Integer,
            carer, #       :: Carer,
            yp, #           :: YP,.yp_contrib_type = no_contribution
            council, # :: DataFrameRow,
            params ) #       :: Params )
        @test outcomes ≈ baseoutcomes
        baseoutcomes = CarerOutcomes( 100.0, 50.0, 25.0, 150.0 )
        params = getdefaultparams()
        settings = default_data_settings()
        year = 2019
        StayingPutSim.overrideoutcomes!(
            outcomes,
            council.ccode,
            year,#         :: Integer,
            carer, #       :: Carer,
            yp, #           :: YP,.yp_contrib_type = no_contribution
            council, # :: DataFrameRow,
            params ) #       :: Params )
        params.yp_contrib_type = no_contribution
        println( "outcomees before no_yp_contrib=$outcomes")
        StayingPutSim.overrideoutcomes!(
            outcomes,
            council.ccode,
            year,#         :: Integer,
            carer, #       :: Carer,
            yp, #           :: YP,.yp_contrib_type = no_contribution
            council, # :: DataFrameRow,
            params ) #       :: Params )

        println( "outcomees after no_yp_contrib=$outcomes")
        @test outcomes.contributions_from_yp == 0
        @test outcomes.income_recieved == baseoutcomes.income_recieved
        @test outcomes.payments_from_la == baseoutcomes.income_recieved

        params.yp_contrib_type = flat_rate
        params.yp_contrib = 10.0
        StayingPutSim.overrideoutcomes!(
            outcomes,
            council.ccode,
            year,#         :: Integer,
            carer, #       :: Carer,
            yp, #           :: YP,.yp_contrib_type = no_contribution
            council, # :: DataFrameRow,
            params ) #       :: Params )

        @test outcomes.contributions_from_yp == 10
        @test outcomes.income_recieved == baseoutcomes.income_recieved
        @test outcomes.payments_from_la == baseoutcomes.income_recieved-10.0

        params = getdefaultparams()
        params.payment = min_payment
        rcode = payclassfromregioncode( council.rcode )
        StayingPutSim.overrideoutcomes!(
            outcomes,
            council.ccode,
            year,#         :: Integer,
            carer, #       :: Carer,
            yp, #           :: YP,.yp_contrib_type = no_contribution
            council, # :: DataFrameRow,
            params ) #       :: Params )

        println( "outcomees after no_yp_contrib=$outcomes")
        @test outcomes.income_recieved==194.0


        # payments_from_la :: Real
        # contributions_from_yp :: Real
        # housing_cont :: Real
        # income_recieved :: Real

    end

end
