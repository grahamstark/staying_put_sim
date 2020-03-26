module CareData
    using StatsBase
    using Statistics

    using FileIO: load
    using DataFrames
    using Query
    using IterableTables
    using CSVFiles
    using IteratorInterfaceExtensions
    import TableTraits: isiterabletable

    using GlobalDecls
    using ONSCodes
    using Utils
    import LAModelData: getstayingrates, get_18s_level_from_doe

    export load_all
    export get_yp, make_yp_frame, make_carer_frame
    export Carer, YP
    export create_base_datasets, addcarertoframe!
    export DataSettings, default_data_settings
    export CPIINDEX, AFC_SURVEY_YEAR, THIS_YEAR, uprate
    export CarerOutcomes, addcareroutcomestoframe!, makecareroutcomesframe
    export fillcomponents!

    THIS_YEAR=2020
    AFC_SURVEY_YEAR=2018

    MINIMUM_WAGE_2019 = 6.15*40 # 18-20
 	JSA_IS_2019 = 57.90

    MINIMUM_WAGE_2020 = 6.45*40 # 18-20
 	JSA_IS_2020 = 57.90 # FIXME unchanged 2019/20 rate

    MINIMUM_WAGE=MINIMUM_WAGE_2020
 	JSA_IS=JSA_IS_2020

    @enum EmploymentStatus TrainingOrEmployment HigherEd OtherEd NEET Unknown
    @enum DataPublisher OFSTED DFE AFC

    export EmploymentStatus, TrainingOrEmployment, HigherEd, OtherEd, NEET, Unknown
    export DataPublisher, OFSTED, DFE, AFC

    function efroms( s :: AbstractString ) :: EmploymentStatus
        t :: EmploymentStatus = Unknown
        if s == "TrainingOrEmployment"
            t = TrainingOrEmployment
        elseif s == "HigherEd"
            t = HigherEd
        elseif s == "OtherEd"
            t = OtherEd
        elseif s == "NEET"
            t = NEET
        else
            t = Unknown
        end
        t
    end

    function efroms( s :: EmploymentStatus)::EmploymentStatus
        s
    end

    #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 whose activity is training or employment
    # from DFe CareLeavers_Act_InTouch19to212018_amended.csv
    CL_All_19to21_2018  = 28_510 # all
    ED_PCS_2018 = (
        #  Care leavers in the year ending 31 March 2018 aged 19 to 21 who were looked after for a total of at least 13 weeks after their 14th birthday including some time after their 16th birthday [1]
        :CL_Act_TE19to21_pc => 25, # 25

        #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 whose activity is higher education i.e. studies beyond A level
        :CL_Act_HE19to21_pc => 6, # HE 6

        #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 whose activity is education other than higher education
        :CL_Act_OE19to21_pc => 20, # FE 20

        # Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 who are not in education, employment or training
        :CL_Act_NEET19to21_pc => 39, # NEET 39

        #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 for whom local authority does not have activity information
        :CL_Act_NoInf19to21_pc => 10 )
         #No inf 10
    # 2019 version
    CL_All_19to21_2019  = 29_930 # all
    ED_PCS_2019 = (
        #  Care leavers in the year ending 31 March 2018 aged 19 to 21 who were looked after for a total of at least 13 weeks after their 14th birthday including some time after their 16th birthday [1]
        :CL_Act_TE19to21_pc => 25.0, # 25

        #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 whose activity is higher education i.e. studies beyond A level
        :CL_Act_HE19to21_pc => 6.0, # HE 6

        #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 whose activity is education other than higher education
        :CL_Act_OE19to21_pc => 21.0, # FE 20

        # Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 who are not in education, employment or training
        :CL_Act_NEET19to21_pc => 39.0, # NEET 39

        #  Percentage of care leavers in the year ending 31 March 2018 aged 19 to 21 for whom local authority does not have activity information
        :CL_Act_NoInf19to21_pc => 9.0 ) #No inf 10

    ED_PCS = ED_PCS_2019 # FIXME move this to a function
    CL_All_19to21 = CL_All_19to21_2019

    function educstatus( ccode :: AbstractString ) :: EmploymentStatus
        # FIXME should use LA level stuff and a search
        # FIXME 2020 needed
        e ::EmploymentStatus = NEET
        r = rand(1:90)
        if( r <= 25)
            e = TrainingOrEmployment
        elseif( r <= 31 )
            e = HigherEd
        elseif( r <= 51 )
            e = OtherEd
        end
        e
    end

    # from ofsted ss 2019, addn table 1
    # we'll use this is proportions in each skill class
    # Approved for one type of care	20,260
    # Approved for two types of care	12,735
    # Approved for three types of care	4,905
    # Approved for four or more types of care	5,575
    # then (Main Table, col1 4920 new approved - assume level 0 )
    SKILLS_LEVELS_2018 = [5000, 20060.0-5000,12735.0,4905.0,5575.0]
    SKILLS_PROPS_2018 = cumsum(SKILLS_LEVELS_2018)/sum(SKILLS_LEVELS_2018)

    # 5_110 is 8400 approved - 3290 friends and family
    SKILLS_LEVELS_2019 = [5_110, 20_755.0-5000,13_510.0,5_200.0,4_985.0]
    SKILLS_PROPS_2019 = cumsum(SKILLS_LEVELS_2019)/sum(SKILLS_LEVELS_2019)

    SKILLS_LEVELS=SKILLS_LEVELS_2019
    SKILLS_PROPS=SKILLS_PROPS_2019

    function skilllevel()
        r = rand()
        nsk = size( SKILLS_PROPS )[1]
        for i in 1:nsk
            if r <= SKILLS_PROPS[i]
                return i
            end
        end
    end

    # actual and forecast CPI index rebased to 2019=100; from ONS chart 3.18
    # FIXME unchanged for update
    CPIINDEX = Dict(
        2010 => 0.816463685631,
        2011 => 0.843392062033,
        2012 => 0.881038496092,
        2013 => 0.905956397748,
        2014 => 0.929192356761,
        2015 => 0.942768148942,
        2016 => 0.943146193059,
        2017 => 0.949367808928,
        2018 => 0.974840594465,
        2019 => 1.00,
        2020 => 1.020495466904,
        2021 => 1.040612242049,
        2022 => 1.062698654012,
        2023 => 1.084820802953,
        2024 => 1.106897548551,
        2025 => 1.129423568993,
        2026 => 1.152408007287,
        2027 => 1.175860192508,
        2028 => 1.199789643583,
        2029 => 1.224206073155,
        2030 => 1.249119391524,
        2031 => 1.274539710671,
        2032 => 1.300030504884,
        2033 => 1.326031114982

    )

    function uprate( x :: Real, newyear :: Integer, oldyear :: Integer = -1 ) :: Real
        if oldyear == -1
            oldyear = newyear-1
        end
        x * (CPIINDEX[newyear]/CPIINDEX[oldyear])
    end

    # from OFSTED
    total_in_placements = [50600,51315,51805,51805,52005,53040]
    total_reaching_18 = [3160,3335,3435,4025,3430,3435]
    age18_and_remain = [1685, 1750, 1790, 2190, 1570, 1695]

    ny = size( total_in_placements )[1]
    # println( "size $ny" )
    growth = zeros( ny-1 )

    for i in 1:ny-1
        growth[i]=1+(total_in_placements[i+1]-total_in_placements[i])/total_in_placements[i]
    end



    function make_yp_frame( n::Integer )
        DataFrame(
            year          = zeros( Integer, n ),
            ccode         = Vector{AbstractString}(undef,n),
            carer         = zeros( Integer, n ),
            age           = zeros( Integer, n ),
            housing_costs = zeros( n ),
            earnings      = zeros( n ),
            benefits      = zeros( n ),
            employment_status = Vector{EmploymentStatus}(undef, n ))
    end

    function make_carer_frame( n::Integer )
        DataFrame(
            year              = zeros( Integer, n ),
            ccode             = Vector{AbstractString}(undef,n),
            id                = zeros( Integer, n ),
            age               = zeros( Integer, n ),
            housing_costs     = zeros( n ),
            earnings          = zeros( n ),
            benefits          = zeros( n ),
            employment_status = zeros( Integer, n ),
            skill_level       = zeros( Integer, n ))
     end

     mutable struct Carer
        id                :: Integer
        age               :: Integer
        housing_costs     :: Real
        earnings          :: Real
        benefits          :: Real
        employment_status :: Integer
        skill_level       :: Integer
     end

     function carerfromrow( i ) :: Carer
         Carer( i.id, i.age, i.housing_costs, i.earnings, i.benefits, i.employment_status, i.skill_level )
     end

     function ypfromrow( q ) :: YP
        YP( q.age, q.housing_costs, q.earnings, q.benefits, efroms( q.employment_status ))
     end


     function getcarers(
        carer_dataset,
        ccode :: AbstractString,
        year  :: Integer ) :: Array{Carer}
        @assert isiterabletable( carer_dataset ) "data needs to implement IterableTables"
        q = @from i in carer_dataset begin
            @where (i.ccode == ccode) && (i.year == year)
            @select i
            @collect
        end
        lq = length( q )[1]
        # println( "lq = $lq")
        carers = Array{Carer}( undef, lq )
        j = 0
        for i in q
            j += 1
            carers[j] = Carer( i.id, i.age, i.housing_costs, i.earnings, i.benefits, i.employment_status, i.skill_level )
        end
        carers
     end # getcarers

    function addcarertoframe!( yp_dataset, year ::Integer, ccode :: AbstractString, carer :: Carer )
        @assert isiterabletable( yp_dataset ) "data needs to implement IterableTables; is "*typeof(yp_dataset)
        d = [ year, ccode, carer.id, carer.age, carer.housing_costs,
             carer.earnings, carer.benefits, carer.employment_status, carer.skill_level]
        push!( yp_dataset, d )
    end

    mutable struct CarerOutcomes
        payments_from_la      :: Real
        income_recieved       :: Real
        contributions_from_yp :: Real
        housing_cont          :: Real
        other_cont            :: Real
        fee                   :: Real
        allowance             :: Real
    end

    function CarerOutcomes(; fee :: Real, allowance::Real, housing_cont :: Real, other_cont :: Real ) :: CarerOutcomes
        c = CarerOutcomes( 0.0, 0.0, 0.0, housing_cont, other_cont, fee, allowance )
        fillcomponents!( c )
        c
    end

    function fillcomponents!( c :: CarerOutcomes )
        c.contributions_from_yp =
            c.housing_cont + c.other_cont
        c.income_recieved = c.fee + c.allowance
        c.payments_from_la = max( 0.0, c.income_recieved - c.contributions_from_yp)
    end

    import Base.≈

    function ≈(left :: CarerOutcomes, right::CarerOutcomes )::Bool
        ( left.payments_from_la ≈ right.payments_from_la ) &&
        ( left.contributions_from_yp ≈ right.contributions_from_yp ) &&
        ( left.housing_cont ≈ right.housing_cont ) &&
        ( left.income_recieved ≈ right.income_recieved )
    end


    function makecareroutcomesframe( n :: Integer )
        DataFrame(
            sysno         = zeros( Integer, n ),
            iteration     = zeros( Integer, n ),
            year          = zeros( Integer, n ),
            ccode         = Vector{AbstractString}(undef,n),
            rcode         = Vector{AbstractString}(undef,n),
            carer         = zeros( Integer, n ),
            yp_age        = zeros( Integer, n ),
            payments_from_la      = zeros( n ),
            contributions_from_yp = zeros( n ),
            income_recieved       = zeros( n )
            )
    end

    function addcareroutcomestoframe!(
        out_dataset,
        sysno :: Integer,
        iteration :: Integer,
        year  :: Integer,
        ccode :: AbstractString,
        rcode :: AbstractString,
        carerid::Integer,
        yp_age :: Integer,
        outcomes :: CarerOutcomes )
        @assert isiterabletable( out_dataset ) "data needs to implement IterableTables; is "*typeof(out_dataset)
        d = [ sysno, iteration, year, ccode, rcode, carerid, yp_age, outcomes.payments_from_la, outcomes.contributions_from_yp, outcomes.income_recieved]
        push!( out_dataset, d )
    end

    function make_council_frame( n::Integer )
        DataFrame(
            name  = Vector{AbstractString}(undef,n),
            ccode = Vector{AbstractString}(undef,n),
            rcode = Vector{AbstractString}(undef,n))
    end

    mutable struct YP
        age :: Integer
        housing_costs :: Real
        earnings      :: Real
        benefits      :: Real
        employment_status :: EmploymentStatus
    end

    function makelaoutcomesframe( n::Integer )
        DataFrame(
            year      = zeros( Integer, n ),
            iteration = zeros( Integer, n ),
            council   = Vector{AbstractString}(undef,n),
            ccode     = Vector{AbstractString}(undef,n),
            rcode     = Vector{AbstractString}(undef,n),
            incomes   = zeros(n),
            payments  = zeros(n),
            grant_allocations = zeros(n))
    end

    function ageyp!( yp :: YP, year :: Integer )
        yp.age += 1
        yp.housing_costs = uprate(yp.housing_costs, year)
        yp.earnings = uprate(yp.earnings, year)
        yp.benefits = uprate(yp.benefits, year)
    end

    function agecarer!( carer :: Carer, year :: Integer )
        carer.age += 1
        carer.housing_costs = uprate(carer.housing_costs, year)
        carer.earnings = uprate(carer.earnings, year)
        carer.benefits = uprate(carer.benefits, year)
    end

    function get_yp(
        yp_dataset,
        year       :: Integer,
        carer      :: Carer,
        yearmatch  = nothing ) :: Union{YP,Nothing}
        @assert isiterabletable( yp_dataset ) "data needs to implement IterableTables"
        hits=[]
        if yearmatch !== nothing
            hits = (yp_dataset.carer .== carer.id) .& yearmatch
        else
            hits = (yp_dataset.carer .== carer.id) .& (yp_dataset.year .== year)
        end
        n = 0
        yp = nothing
        for q in eachrow(yp_dataset[hits,:])
            yp = YP( q.age, q.housing_costs, q.earnings, q.benefits, q.employment_status )
            n += 1
        end
        @assert n <= 1
        yp
    end

    """
    looks up a random cat A LHA allowance for this LA. (Random since you can have > 1 BHA for each LA)
    if missing returns the mean of all values in CAT A for 2019  LHA Tables 2019/20
    see: https://www.gov.uk/government/publications/local-housing-allowance-lha-rates-applicable-from-april-2019-to-march-2020
    see: http://sticerd.lse.ac.uk/case/ for what we really want to do here
    """
    function get_shared_accommodation_rate( ccode :: AbstractString )::Real
        v = ONSCodes.getbravalue( ccode )
        if v < 0.0
            v = 69.16
        end
        v
    end



    function get_yp2( yp_dataset, year :: Integer, carer :: Carer ) :: Union{YP,Nothing}
        @assert isiterabletable( yp_dataset ) "data needs to implement IterableTables"
        q = @from i in yp_dataset begin
            @where i.carer == carer.id && i.year == year
            @select i
            @collect
        end
        n = size( q )[1]
        @assert n <= 1
        if n == 1
            q = q[1]  # handle as a dataframe row
            YP( q.age, q.housing_costs, q.earnings, q.benefits, q.employment_status )
        end
    end

    function addyptoframe!( yp_dataset, year ::Integer, ccode :: AbstractString, carerid::Integer, q :: YP )
        @assert isiterabletable( yp_dataset ) "data needs to implement IterableTables"
        d = [ year, ccode, carerid, q.age, q.housing_costs, q.earnings, q.benefits, q.employment_status]
        push!( yp_dataset, d )
    end

    function newperson()::YP
        YP( 18,0.0,0.0, Unknown )
    end

    mutable struct DataSettings
        name      :: AbstractString
        dataset   :: AbstractString
        description :: AbstractString
        agglevel  :: AggLevel
        poolyears :: Bool
        avggrowth :: Real
        prp_reach_18 :: Real
        startyear :: Integer
        endyear   :: Integer
        annual_sp_increment :: Real
        num_iterations :: Integer
        targets   :: Array{AbstractString}
        reaching_18s_source :: DataPublisher
        datayear  :: Integer
    end

    function default_data_settings()::DataSettings
        DataSettings(
            "",
            "",
            "Description of the run",
            local_authority,
            false,
            mean(growth),
            (total_reaching_18./total_in_placements)[ny],
            2017,
            2026,
            0.0,
            1,
            [],
            OFSTED,
            THIS_YEAR )
    end

    function addageddata!(
        ;
        yp_dataset,
        carer_dataset,
        pid       :: Integer,
        ccode     :: AbstractString,
        ladata    :: Dict,
        stayingrates :: Vector{<:Real},
        startyear :: Integer
        )
        @assert isiterabletable( yp_dataset ) "yp_dataset needs to implement IterableTables"
        @assert isiterabletable( carer_dataset ) "carer_dataset needs to implement IterableTables"

        employment_status = educstatus( ccode )
        housing = get_shared_accommodation_rate( ccode )*CPIINDEX[startyear]

        benefits = 0.0
        earnings = 0.0
        if employment_status == TrainingOrEmployment
            earnings = MINIMUM_WAGE*CPIINDEX[startyear]
        end
        benefits = 0.0
        if employment_status == NEET
            benefits = JSA_IS*CPIINDEX[startyear]
        end
        yp = YP( 18, housing, earnings, benefits, employment_status )
        earnings = 0.0
        employment_status = 0
        skill_level = skilllevel()
        housing = 0.0
        benefits = 0.0
        carer = Carer( pid, 40, housing, earnings, benefits, employment_status, skill_level )
        thisyear = startyear
        println( "stayingrates=$stayingrates")
        for p in 1:3 ### FIXME parameterise 3
            r = rand()
            stayr = stayingrates[p]
            if r > stayr
                println( "exiting; r=$r stayrate $stayr ")
                break
            end
            addyptoframe!( yp_dataset, thisyear, ccode, carer.id, yp )
            addcarertoframe!( carer_dataset, thisyear, ccode, carer )
            ageyp!( yp, thisyear )
            agecarer!( carer, thisyear )
            thisyear += 1
        end
    end # addageddata!

    function create_base_datasets(
        ofdata   :: DataFrame,
        settings :: DataSettings )
        if size(settings.targets)[1] > 0
            targets  = ofdata.ccode .|> [x->x in settings.targets]
            t = settings.targets
            println( "settings.targets = $t")
            println( "targets = $targets")
            ofdata = ofdata[targets,:]
        end
        println( "settings $settings")
        ncouncils = size( ofdata )[1]
        councils = make_council_frame( ncouncils )
        carer_data = make_carer_frame(0)
        yp_data = make_yp_frame(0)
        nc = 0
        pid = 0
        r = 0
        for ofdat in eachrow(ofdata)
            r += 1
            ccode = ofdat.ccode
            if (! ONSCodes.isaggregate( ccode )) && (! (ccode in SKIPLIST ) )
                nc += 1
                if nc > 2000 # test break if needed
                    break
                end
                councils[:name][nc] = ofdat[:council]
                councils[:ccode][nc] = ccode
                councils[:rcode][nc] = ofdat[:rcode]
                base_staying_rates = getstayingrates(
                    ccode,
                    settings.agglevel,
                    settings.poolyears )
                total_fostered = ofdat[:number_of_children_or_young_people_in_placements_at_31_march]
                if settings.reaching_18s_source == OFSTED
                    new_reached_18_base = settings.prp_reach_18 * total_fostered
                else
                    new_reached_18_base = get_18s_level_from_doe( ccode )
                    if ismissing( new_reached_18_base )
                        new_reached_18_base = settings.prp_reach_18 * total_fostered # fallback
                    end
                end
                g = 1.0
                for year in (settings.startyear-2):settings.endyear # FIXME hack
                    y = year-2019 # fixme make 2019 some sort of parameter
                    stayingrates = base_staying_rates .+ ( settings.annual_sp_increment*y )
                    println( "at year $year stayingrates = $stayingrates base_staying_rates $base_staying_rates")
                    g = settings.avggrowth^y
                    new_reached_18 = round(Integer,new_reached_18_base*g)
                    println( "adding $new_reached_18 people for council $ccode total fostered $total_fostered y=$y g=$g new_reached_18_base=$new_reached_18_base")
                    for i in 1:new_reached_18
                        pid += 1
                        addageddata!(
                            yp_dataset = yp_data,
                            carer_dataset = carer_data,
                            pid=pid,
                            ccode=ccode,
                            ladata = Dict(),
                            stayingrates=stayingrates,
                            startyear=year
                        )
                    end # people
                end # year
            end # a valid council
        end # councils
        year_matches = Dict()
        for year in (settings.startyear-2):settings.endyear+2
            year_matches[year] = yp_data.year .== year
        end
        (
            councils     = councils,
            carer_data   = carer_data,
            yp_data      = yp_data,
            year_matches = year_matches )
    end

    function load_all( year :: Integer )::NamedTuple
        ofdata = load( DATADIR*"edited/$(year)/OFDATA.csv" ) |> DataFrame
        lcnames = Symbol.(Utils.basiccensor.(string.(names(ofdata))))
        rename!(ofdata, lcnames )
        # loadtable( DATADIR*"edited/OFDATA.csv", indexcols=[:ccode] )
        grantdata = load( DATADIR*"edited/$(year)GRANTS_$(year).csv" ) |> DataFrame
        # loadtable(  DATADIR*"edited/GRANTS_2019.csv", indexcols=[:ccode] )
        lcnames = Symbol.(Utils.basiccensor.(string.(names(grantdata))))
        rename!(grantdata, lcnames )
        (ofdata=ofdata, grantdata=grantdata )
    end # load_all

end
