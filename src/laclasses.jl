using DataFrames
using CSV
using DataFramesMeta
using Query
using DataFramesMeta


include( "utils.jl" )

@enum AggLevel national regional la

function loadone( name :: AbstractString ) :: DataFrame
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
function getunderlyingdata()::NamedTuple
    (
    c17_18_2017=loadone(
            DATADIR*"underlying_data/2017/SFR50_CareLeavers17182017.csv" ),
    c19_21_2017 = loadone(
            DATADIR*"underlying_data/2017/SFR50_CareLeavers19to212017.csv" ),
    c17_18_2018 = loadone(
            DATADIR*"underlying_data/2018/CareLeavers17182018_amended.csv" ),
    c19_21_2018 = loadone(
            DATADIR*"underlying_data/2018/CareLeavers_Acc_StayPut19to212018.csv" )
    )
end

UDATA = getunderlyingdata()


"""
extract the number leaving care and the number staying put 
for the given target (LA, country, etc.)
age (18,19,20) and year (2017,2018)
"""
function doquery( 
    year   :: Integer, 
    target :: AbstractString, 
    age    :: Integer )
    as = age == 18 ? "17_18" : "19_21"
    field = Symbol( "c"*as*"_"*"$year" ) 
    f1 = Symbol( "cl_stayput_$age" )
    f2 = Symbol( "cl_stayput_ffc_$age" )
    println( "f1 $f1 f2 $f2 field $field" )
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
 return ONS code of either the LA, its region, or UK, depending on `AggLevel`
"""
function gettargetla( 
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
function getexitrates( 
    lacode    :: AbstractString, 
    agglev    :: AggLevel, 
    poolyears :: Bool  ) :: Vector
    println( "getexitrates for lacode $lacode agglev $agglev" );
    avprop = zeros(3)
    target = gettargetla( lacode, agglev )
    ages = [18,19,20]
    years = poolyears ? [2017,2018] : [2018]
    for year in years
        i = 0
        numleaving=zeros(3)
        numstaying=zeros(3)
        prop=zeros(3)
        for age in ages
            i+=1
            q = doquery( year, target, age )
            if zeroormissing( q )
                @assert agglev == la
                target = gettargetla( lacode, regional )
                println( "zeros detected! lacode $lacode new target $target $year age $age " );
                q = doquery( year, target, age )
            end
            @assert (! zeroormissing( q ))
            numleaving[i] += get(q[1],0)
            numstaying[i] += get(q[2],0)
        end
        prop = numstaying./numleaving
        prop[2] /= prop[1]
        prop[3] /= prop[2]
        println( "prop $prop " );
        println( "numstaying $numstaying numleaving $numleaving" )
        avprop .+= prop
    end
    avprop /= length( years )
    # t = (a18=avprop[1],a19=avprop[2],a20=avprop[3])
    return avprop
end



# df meta example:
# @linq las |> where( :LAD19NM.=="City of London" ) |> select( :LAD19CD )


@enum Region london se rest_of_england

function payclassfromregioncode( regcode :: AbstractString ) :: Region
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

function isaggregate( ccode )
    if ccode === missing 
        return true
    end
    if ccode == Nothing
        return true
    end
    if length(ccode)==0 
        return true
    end
    if ccode[1:3] in ["E12", "E92" ] 
        return true
    end
    return false
    
#     return name in [
#         "East of England",
#         "England",
#         "North East England",
#         "North West England",
#         "Yorkshire and The Humber",
#         "East Midlands",
#         "West Midlands",
#         "London",
#         "Inner London",
#         "Outer London",
#         "South East England",
#         "South West England",
#         "London Tri-borough" ]
end

