module LAModelData

    using DataFrames
    using CSV
    using DataFramesMeta
    using Query

    using GlobalDecls
    using Utils
    using ONSCodes

    export is_aggregate, get_pay_class_from_region_code
    export get_staying_put_rates, get_target_la, do_exit_rate_query
    export UDATA, get_18s_level_from_doe

    function load_one( name :: AbstractString ) :: DataFrame
        df = CSV.File(
            name,
            delim=',',
            missingstrings=["x","","-"],
            types=maketypeblock(6:1000)) |> DataFrame
        lcnames = Symbol.(lowercase.(string.(names(df))))
        names!(df,lcnames)
        df
    end


    #
    # DFE Raw data on care movements 2015-2018; see
    # only 2017 and 2018 have Staying Put data
    #
    function get_underlying_data()::NamedTuple
        (
        c17_18_2017=load_one(
                DATADIR*"underlying_data/2017/SFR50_CareLeavers17182017.csv" ),
        c19_21_2017 = load_one(
                DATADIR*"underlying_data/2017/SFR50_CareLeavers19to212017.csv" ),
        c17_18_2018 = load_one(
                DATADIR*"underlying_data/2018/CareLeavers17182018_amended.csv" ),
        c19_21_2018 = load_one(
                DATADIR*"underlying_data/2018/CareLeavers_Acc_StayPut19to212018.csv" )
        )
    end

    UDATA = get_underlying_data()

    """
    get the number of 18s leaving care from the DoE underlying data as an
    alternative to a calculation from OFSTED data and ageing rates
    """
    function get_18s_level_from_doe( target :: AbstractString )
        which = UDATA.c17_18_2018.new_geog_code .== target
        v = UDATA.c17_18_2018[which,:cl_stayput_18][1]
        #if( ismissing( v ))
    #        v=-1
        #end
        return v

        q = @from i in UDATA.c17_18_2018 begin
            @where i.new_geog_code == target
            @select i.cl_stayput_18
            @collect
        end
        q[1].value
    end


    """
    extract the number leaving care and the number staying put
    for the given target (LA, country, etc.)
    age (18,19,20) and year (2017,2018)
    """
    function do_exit_rate_query(
        year   :: Integer,
        target :: AbstractString,
        age    :: Integer )
        as = age == 18 ? "17_18" : "19_21"
        field = Symbol( "c"*as*"_"*"$year" )
        f1 = Symbol( "cl_stayput_$age" )
        f2 = Symbol( "cl_stayput_ffc_$age" )
        # println( "target $target age $age f1 $f1 f2 $f2 field $field" )
        q = @from i in UDATA[field] begin
            @where i.new_geog_code == target
            @select i[f1], i[f2]
            @collect
        end
        # @assert length( q ) == 1 no - 3 londons ffs we use London not Inner/Outer London
        println( "q1 $q[1]" )
        q[1]
    end

    """
     return ONS code of either the LA, its region, or England, depending on `AggLevel`
    """
    function get_target_la(
        lacode :: AbstractString,
        agglev :: AggLevel ) :: AbstractString
        target = lacode
        if agglev == regional
            target = regioncodefromcode( lacode )
        elseif agglev == national
            target = "E92000001" # england
        end
        target
    end


    """
    return the exit rates for the given LA - a named tuple a18,a19,a20
      will be the regional one if the calculation is not possible for an LA
      rates are proportions for that age - so 0.5,0.5,0.5 means 50% stay 1 year, 25% 2 12.5% 3 and so on
    """
    function get_staying_put_rates(
        lacode    :: AbstractString,
        agglev    :: AggLevel,
        poolyears :: Bool  ) :: Vector
        # println( "getexitrates for lacode $lacode agglev $agglev" );
        avprop = zeros(3)
        target = get_target_la( lacode, agglev )
        ages = [18,19,20]
        years = poolyears ? [2017,2018] : [2018]
        for year in years
            i = 0
            numreachingage=zeros(3)
            numstaying=zeros(3)
            prop=zeros(3)
            for age in ages
                i+=1
                q = do_exit_rate_query( year, target, age )
                if zeroormissing( q )
                    @assert agglev == local_authority
                    target = get_target_la( lacode, regional )
                    println( "zeros detected! lacode $lacode new target $target $year age $age " );
                    q = do_exit_rate_query( year, target, age )
                end
                @assert (! zeroormissing( q ))
                numreachingage[i] += get(q[1],0)
                numstaying[i] += get(q[2],0)
            end
            prop = zeros(3)
            #
            # FIXME this goes wrong if 1 year is region and the
            # other is LA
            #
            println( "got exit data numstaying = $numstaying numreachingage $numreachingage")

            # prop[1] = numstaying[1]/numreachingage[1]
            # prop[2] = numstaying[2]/numstaying[1]
            # prop[3] = numstaying[3]/numstaying[2]
            # correct way, I think - prop is scale
            prop = numstaying./numreachingage
            prop[2] /= prop[1]
            prop[3] /= (prop[2]*prop[1])
            # println( "prop $prop " );
            # println( "numstaying $numstaying numreachingage $numreachingage" )
            avprop .+= prop
        end
        avprop /= length( years )
        # t = (a18=avprop[1],a19=avprop[2],a20=avprop[3])
        return avprop
    end



    # df meta example:
    # @linq las |> where( :LAD19NM.=="City of London" ) |> select( :LAD19CD )

    function get_pay_class_from_region_code( regcode :: AbstractString ) :: Region
        # E12000001  #	A 	North East England
        # E12000002  # 	B 	North West England
        # E12000003  #	D 	Yorkshire and the Humber
        # E12000004  #	E 	East Midlands
        # E12000005  # 	F 	West Midlands
        # E12000006  # 	G 	East of England
        if regcode == "E12000007"
            r = london # 	H 	London
        elseif regcode == "E12000008"
            r = se
            # J 	South East England
        else
            r = rest_of_england
        end
        # E12000009 	K 	South West England
        r
    end

    function is_aggregate( name )
        return name in [
            "East of England",
            "England",
            "North East England",
            "North West England",
            "Yorkshire and The Humber",
            "East Midlands",
            "West Midlands",
            "London",
            "Inner London",
            "Outer London",
            "South East England",
            "South West England",
            "London Tri-borough" ]
    end

end # LAModelData
