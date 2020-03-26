module StayingPutSim


    using DataFrames
    using StatFiles
    using IterableTables
    using IteratorInterfaceExtensions
    using TableTraits
    using CSV
    using GLM
    # using Plots
    using StatsModels
    using StatsBase
    using Statistics
    using Query
    using DataFramesMeta

    using TableTraits

    # local imports
    using Utils
    using GlobalDecls

    #import CareData: Carer, YP, create_base_datasets, DataSettings, get_yp, make_yp_frame,
#        make_carer_frame,  DataSettings, CPIINDEX, CarerOutcomes, uprate,  AFC_SURVEY_YEAR,
#        EmploymentStatus

    using CareData
    using FosterParameters
    using LAModelData: get_pay_class_from_region_code
    export do_one_calc, override_outcomes!, do_basic_calc, trackseries

    function do_basic_calc(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes

        fee1 = ismissing(council_data[:sp_fee_1]) ? 0.0 : council_data[:sp_fee_1]
        allow1 = ismissing(council_data[:sp_allowance_1]) ? 0.0 : council_data[:sp_allowance_1]
        # fixme do something with these
        fee2 = ismissing(council_data[:sp_fee_2]) ? 0.0 : council_data[:sp_fee_2]
        allow2 = ismissing(council_data[:sp_allowance_2]) ? 0.0 : council_data[:sp_allowance_2]
        fee3 = ismissing(council_data[:sp_fee_3]) ? 0.0 : council_data[:sp_fee_3]
        allow3 = ismissing(council_data[:sp_allowance_3]) ? 0.0 : council_data[:sp_allowance_3]
        fee4 = ismissing(council_data[:sp_fee_3]) ? 0.0 : council_data[:sp_fee_4]

        fee1 = uprate( fee1, year, AFC_SURVEY_YEAR )
        allow1 = uprate( allow1, year, AFC_SURVEY_YEAR )
        CarerOutcomes(
            fee          = fee1,
            allowance    = allow1,
            housing_cont = 0.0,
            other_cont   = 0.0 )
    end # do_basic_calc

    FUNCS = Dict()

    # so taper(10,[1.0,0.5,0.25]) => 10, 5, 2.5
    function taper( amount :: Float64, rates :: Vector{Float64} ) :: Vector{Float64}
        n = size( rates )[1]
        out .*= rates*amount
        out
    end

    function darlington(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes ## return type illegal here
        println("Darlington ($ccode)")
        # allowance £246.47 a week plus fee until two weeks after a young person completes their 13th year of education.
        # Afterwards it reduces to the normal rate of £160.00 a week plus £20.00 from the young person.
        # since 13th yearis 18, assume 160
        allow = uprate(160.00,year,AFC_SURVEY_YEAR)
        contributions = uprate(20.00,year,AFC_SURVEY_YEAR)
        # bands A,A+topup,B,C
        fees = uprate.([0, 75.0,90,100,150],year,AFC_SURVEY_YEAR)
        fee = fees[carer.skill_level]
        CarerOutcomes(
            fee          = fee,
            allowance    = allow,
            housing_cont = 0.0,
            other_cont   = contributions )
        # CarerOutcomes( payments, contributions, 0.0, max(0.0,payments+contributions ), fee )
    end

    function sefton(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        hcontrib = uprate( 50.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = min( hcontrib, yp.housing_costs )
            outcomes.other_cont = uprate( 12.0, year, AFC_SURVEY_YEAR )
        end
        outcomes
    end

    function oldham_style_fee(
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        fees         :: Vector ) :: Real
        ufees = uprate.( fees, year, AFC_SURVEY_YEAR  )
        fee = ufees[carer.skill_level]
        if yp.age == 19
            fee /= 2
        elseif yp.age == 20
            fee = 0.0
        end
        fee
    end

    function random_oldham_fee(
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        fees         :: Vector ) :: Real
        fee = oldham_style_fee( year, carer, yp, fees )
        prop = 0.86 # see AFC note
        if rand() > prop
            fee = 0.0
        end
        fee
    end

    """
    Year one –Full payment allowance and fee
    Year 2- Full allowance plus half the skill payment
    Year 3- Full allowance
    """
    function oldham(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        fees = uprate.( [0, 78.38, 158.76, 237.23, 340.76], AFC_SURVEY_YEAR  )
        fee = fees[carer.skill_level]
        if yp.age == 19
            fee /= 2
        elseif yp.age == 20
            fee = 0.0
        end
        outcomes.fee = fee
        outcomes
    end

    function rochdale(
            ccode        :: AbstractString,
            year         :: Integer,
            carer        :: Carer,
            yp           :: YP,
            council_data :: DataFrameRow,
            params       :: Params ) :: CarerOutcomes

        # This sounds mich the same as Sefron, so ..
        outcomes = sefton( ccode, year, carer, yp, council_data, params )
        # The ‘Staying Put’ Allowance therefore is based on the former foster placement rate, which would be
        # applied on the young person’s 18th birthday minus the pocket money and clothing allowance element
        # as this should be replaced by the young person’s welfare benefit claim. The ‘Staying Put’ allowance is funded
        # from a number of sources including Housing and other Benefits, Young Person’s Contribution,
        # Personalised Budget and Local Authority. The ‘Staying Put’ Carer will receive the
        # boarding out rate minus pocket money and clothing element on a weekly basis. They will not receive contributions
        # in respect of Christmas or Festival payments, Birthday payment or Holiday payment in order to ensure equity with other care leavers.
        # so.. assume 1/2 comes from other sources GKS
        # outcomes.payments_from_la /= 2.0 #
        outcomes
    end

    function tameside(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        # Staying Put is paid at the same rate as supported
        # lodging which is £177.76 plus £22 contribution from the
        # young person Total £199.76
        outcomes.other_cont = uprate( 22.0, year,  AFC_SURVEY_YEAR )
        outcomes
    end

    function wirral(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        paylim = uprate( 50.0, year,  AFC_SURVEY_YEAR )
        outcomes.other_cont = max(0.0, 0.5*(yp.earnings - paylim))
        outcomes
    end

    function derby(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        outcomes.other_cont = uprate( 12.0, year, AFC_SURVEY_YEAR )
        # 241.72 less YP & HB payments
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes
    end

    """
    Staying Put is based on the rate we are paying when a young person reaches
    18 with a deduction of £23 which the young person is expected to
    contribute and any housing benefit that is payable.
    """
    function derbyshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        outcomes.other_cont = uprate( 23.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes
    end

    """
    The weekly allowance is £80.00 Per week with an additional £86.30 Per week (£166.30) housing
    benefit depending on circumstances the £86.30  may be paid by the Local Authority.
    (essentially if a young person cannot claim housing benefit). This payment remains the same for any period that a Young person is on the Staying Put scheme
    """
    function leicester(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes
    end

    """
        We do not operate a fixed payment level / band. Calculations are based on
        what the foster carers would have been getting as a carer minus housing
        benefit for their local area and a contribution by young person (£30),
    """
    function lincolnshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        outcomes.other_cont = uprate( 30.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes
    end

    """
     £118.00 per week. £210 (£118.00 + £92.00) if the young person does not receive housing benefits.
    """
    function coventry(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        hcontrib = uprate( 92.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = min( hcontrib, yp.housing_costs )
        end
        outcomes
    end

    """
    Basic 195.00. Hybrid 245.00. High Needs 295.00.
    """
    function shropshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        levels = uprate.([195.0, 245.0, 295.0 ], year, AFC_SURVEY_YEAR )
        allowance = assignrand( [0.333,0.666,1.0], levels )
        CarerOutcomes(
            fee          = 0.0,
            allowance    = allowance,
            housing_cont = 0.0,
            other_cont   = 0.0 )
    end

    """
    216.42. Comprising: Contribution from the Young Person's wages/JSA. 25.13.
    100% of the Young Person's housing benefit entitlement (variable) with the balance met by Children's Social Services.
    191.29

    """
    function solihull(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 25.13, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes.other_cont = othcontrib
        outcomes
    end

    """
    100 + c. 60 housing benefit
    """
    function stokeontrent(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        hcontrib = uprate( 60.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = min( hcontrib, yp.housing_costs )
        end
        outcomes
    end

    """
    Staying Put is a fixed rate of £232 per week, and does not change each year.
    The young person should apply for Housing Benefit (unless the carer is a relative or
    claiming Housing Benefit in their own right).  The amount awarded to the yp in Housing Benefit is then deducted from the £232.00.
    """
    function walsall(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes
    end

    """
     Warwickshire County council pays an allowance of £180.00 per week for 'Staying Put' carers,
     but the Young Person pays a contribution if they are working.
    """
    function warwickshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        if yp.earnings > 0.0
            outcomes.other_cont = yp.earnings * 0.5 # FIXME completely made up number
        end
        outcomes
    end

    """
    Payments are static irrespective of year one, two or three. Maximum care payment is either £255.00 or £331.50 depending on level of Foster Carer.
    It is broken down into three components totalling a maximum care payment:
    1. Local Housing Allowance for accommodating Authority e.g. Wolverhampton is £86.30
    2. Contribution from young person - £25.00.
    3. Care Payment from Wolverhampton to total up to maximum care payment.
    """
    function wolverhampton(
            ccode        :: AbstractString,
            year         :: Integer,
            carer        :: Carer,
            yp           :: YP,
            council_data :: DataFrameRow,
            params       :: Params ) :: CarerOutcomes
            outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
            if carer.skill_level > 3
                outcomes.allowance = uprate( 331.50, year, AFC_SURVEY_YEAR )
            end
            othcontrib = uprate( 25.00, year, AFC_SURVEY_YEAR )
            if yp.employment_status == NEET
                outcomes.housing_cont = yp.housing_costs
            end
            outcomes.other_cont = othcontrib
            outcomes
    end


    """
    £137.50 per week if young person is receiving housing benefit and £197.50 if young
    person is not receiving housing benefit.
    This is a flat weekly rate during the staying put agreement.
    """
    function worcestershire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        hcontrib = uprate( 60.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = min( hcontrib, yp.housing_costs )
        end
        outcomes
    end

    """
    We are supporting five  tier 1, one tier 2, and three tier 3.  we also have 2 with no tier group.#
    For the first 4 weeks we pay the full fostering  payment while the client is arranging benefits.
    After 4 weeks the staying put payments are made up of housing benefit approx. £111.00 per week,
    The carer receives their Tier payment, The  carer receives £25.00 for food & £20.00 utilities.
    FIXME is this what that means?
    """
    function luton(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes
    end


    """
        # Standard Allowance£271.08
        # Pocket Money-£12.00
        # Clothing Allowance-£19.39
        # Savings-£20.00
        # TOTAL£219.69
        FIXME WTF
    """
    function bedford(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        outcomes
    end


    function bracknellforest(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        payments = uprate.([160.0, 229, 298, 367, 506.0 ], year, AFC_SURVEY_YEAR )
        allowance = payments[carer.skill_level]
        CarerOutcomes(
            fee=0,
            allowance=allowance,
            housing_cont = 0,
            other_cont= 0 )
    end

    """
      16+ fostering allowance  = £242.08 per week - £34.41.(PA) = £207.67, Housing Benefit = £70 per week (for example), CSD contribution is £207.67 - £70 = £137.67 per week.
    """
    function hampshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 34.41, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes.other_cont = othcontrib
        outcomes
    end

    function medway(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        allowances = uprate.([142.45, 194.11, 226.52 ], year, AFC_SURVEY_YEAR )
        allowance = assignrand( [0.333,0.666,1.0], allowances )
        CarerOutcomes(
            fee=0,
            allowance=allowance,
            housing_cont = 0,
            other_cont= 0 )
    end

    """
    Each carer will receive £200 per week (Staying Put Rate -2017-18) Some may receive £180 per week and the young person makes a £20 contribution towards this weekly.
    """
    function swindon(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 20.0, year, AFC_SURVEY_YEAR )
        outcomes.other_cont = othcontrib
        outcomes
    end

    """
      16+ fostering allowance  = £242.08 per week - £34.41.(PA) = £207.67, Housing Benefit = £70 per week (for example), CSD contribution is £207.67 - £70 = £137.67 per week.
    """
    function wiltshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 20.0, year, AFC_SURVEY_YEAR )
        outcomes.other_cont = othcontrib
        outcomes
    end

    ## FIXME random band fee took middle
    FUNCS["XXHerefordshire"]=function( cohort :: Vector, data :: DataFrameRow, params :: Params )
        frame = do_basic_calc( "Herefordshire", cohort, data, params )
        # £118.00 per week. £210 (£118.00 + £92.00) if the young person does not receive housing benefits.
    end

    function westsussex(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 10.0, year, AFC_SURVEY_YEAR )
        outcomes.other_cont = othcontrib
        outcomes
    end

    function bathandnesomerset(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 13.0, year, AFC_SURVEY_YEAR )
        outcomes.other_cont = othcontrib
        outcomes
    end

    ## FIXME come back & see if I can make some sense out of Wigan
    FUNCS["XXWigan"]=function( cohort :: Vector, data :: DataFrameRow, params :: Params )
        frame = do_basic_calc( "Wigan", cohort, data, params )
        # Year 1. We have 2 rates within the first year 1 – Standard Staying Put Rate of £170.00
        # per week 2 – Providers are paid their current Fostering rate until the end of the Academic Year – these fees range between £307 and £586 per week dependant on what Tier they are on as a foster carer.
        # Years 2 & 3. We pay all Staying Put arrangements standard fee of £170.00 per week.

        fostered_by_age = round.( Integer, cohort )
        num_fostered = sum( fostered_by_age )
        frame = makeframe( "Wigan", data[:ccode], data[:rcode], num_fostered )
        y_1 = fostered_by_age[1]
        after_y_1 = sum( fostered_by_age[2:params.num_years] )
        for i in (y_1+1):params.num_years
            frame.payments[i] = 170.0;
        end
        frame
    end

    """
    #support costs for Project carers if high support needs over the age of 18#
    #£66.70 per week rent - payable by Housing Benefit (or Staying Put budget
    # if HB claim not possible)
    # £30.00 per week contribution from the young person for food and utilities. 17 on standard level and 1 on enhanced
    """
    function dorset(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        hcontrib = uprate( 66.70, year, AFC_SURVEY_YEAR )
        othcontrib = uprate( 30.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = min( hcontrib, yp.housing_costs )
            outcomes.other_cont = outcomes.housing_cont
        end
        outcomes
    end

    function somerset(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        if yp.employment_status == NEET
            outcomes.other_cont = yp.benefits
        end
        outcomes
    end


    function byage(
        yp           :: YP,
        payments     :: Vector,
        year         :: Integer
         ) :: CarerOutcomes
         payments = uprate.(payments, year, AFC_SURVEY_YEAR )
         i = yp.age - 17
         allowance = payments[i]
         CarerOutcomes(
            fee=0,allowance=allowance,housing_cont =0, other_cont=0 )
    end

    """
    Year 1 (18-19 yrs) - £250
    Year 2 (19-20 yrs) - £230
    Year 3 (20-21 yrs) - £180
    """
    function cambridgeshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        allowances = uprate.( [250, 230, 180 ], year, AFC_SURVEY_YEAR )
        byage( yp, allowances, year )
    end


    """
    For carers accredited at Levels 1 to 4, the carer’s total payment in respect
    of the placement will remain in line with their accredited status (i.e. as if
    they were caring for a looked after child aged 17).
    Payments to carers accredited at Level 5 will be capped at the Level 4 rate.
    NCC will pay the carer the appropriate accreditation level minus the rent
    liability for the young person (which is set at the one bedroom
    self-contained Local Housing Allowance) and £10pw utilities.
    100pw retainer while at uni
    """
    function norfolk(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes

        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        othcontrib = uprate( 10.0, year, AFC_SURVEY_YEAR )
        if yp.employment_status == NEET
            outcomes.housing_cont = yp.housing_costs
        end
        outcomes.other_cont = othcontrib
        outcomes
    end


    """
    The Staying Put Policy matches the current arrangements for foster carers
    for children aged 16+, including the skills element but minus pocket
    money, clothing and holiday (£303.10). Staying Put providers receive the
    full skills payment in the first year of a Staying Put arrangement, 75% in
    the second year and 50% in year three.
    """
    function buckinghamshire(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        othcontrib = uprate( 10.0, year, AFC_SURVEY_YEAR )
        outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
        if yp.age == 19
            outcomes.allowance *= 0.75
        elseif yp.age == 20
            outcomes.allowance *= 0.5
        end
        outcomes
    end

    """
    No allowances are paid directly to the former foster carer, a subsistence
    allowance is paid directly to the young person

    Former foster carers are paid on the relevant Band fee that they were
    previously on or remain on as foster carers,
    (see table in response to Q3) there is no differentiation in subsequent years,
    and they continue to be paid at the same rate throughout the Staying Put
    arrangement. 7 carers.
    """
    function ealing(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        fee = oldham_style_fee( year, carer, yp, [0, 78.38, 158.76, 237.23, 340.76] )
        CarerOutcomes( fee=fee, allowance=0.0, housing_cont =0, other_cont=0)
    end

    """
    The Carer would receive the amount/tier level as per the attached that they were on previous to becoming Staying Put minus the below income:-
    Income support.
    Housing Benefit.
    Wages.
    £11.00 utility bills.
    The Staying Put Policy is completed over 2 Stages not in 3 year stages.  Stage 1 they receive allowance and fee.Stage 2 minus the fee.
    """
    function suffolk(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
    outcomes = do_basic_calc( ccode, year, carer, yp, council_data, params )
    othcontrib = uprate( 11.0, year, AFC_SURVEY_YEAR )
    if yp.employment_status == NEET
        outcomes.housing_cont = yp.housing_costs
        outcomes.other_cont = yp.benefits
    end
    outcomes
end

    # TODO simple assign by age West Beds 248.67 198.67 148.67 ??
    # TODO Great Matrix Suffolk
    # TODO Brighton
    # TODO Kent
    # TODO Bristol
    # TODO Bournemouth
    # TODO North Somerset
    # TODO Plymouth
    # TODO POOLE academic year
    # TODO Somerset Fee structure



    FUNCS["E06000005"]=darlington
    FUNCS["E08000005"]=rochdale
    FUNCS["E08000014"]=sefton
    FUNCS["E08000008"]=tameside
    FUNCS["E08000015"]=wirral
    FUNCS["E06000015"]=derby
    FUNCS["E10000007"]=derbyshire
    FUNCS["E06000016"]=leicester
    FUNCS["E10000019"]=lincolnshire
    FUNCS["E08000026"]=coventry
    FUNCS["E06000051"]=shropshire
    FUNCS["E08000029"]=solihull
    FUNCS["E06000021"]=stokeontrent
    FUNCS["E08000030"]=walsall
    FUNCS["E10000031"]=warwickshire
    FUNCS["E08000031"]=wolverhampton
    FUNCS["E10000034"]=worcestershire
    FUNCS["E06000032"]=luton
    FUNCS["E06000055"]=bedford
    FUNCS["E06000036"]=bracknellforest
    FUNCS["E10000014"]=hampshire
    FUNCS["E06000035"]=medway
    FUNCS["E06000030"]=swindon
    FUNCS["E06000054"]=wiltshire
    FUNCS["E10000032"]=westsussex
    FUNCS["E06000022"]=bathandnesomerset
    FUNCS["E10000009"]=dorset
    FUNCS["E10000027"]=somerset
    FUNCS["E10000003"]=cambridgeshire
    FUNCS["E08000004"]=oldham
    FUNCS["E10000020"]=norfolk
    FUNCS["E10000002"]=buckinghamshire

    FUNCS["E10000029"]= suffolk
    FUNCS["E09000009"]= ealing


    # @enum ContributionType actual_contribution no_contribution all_people benefits_only flat_rate
    # @enum PaymentType actual_payment min_payment age_16_17
    # @enum UpratingType no_uprating by_benefits by_cpi

    # now override
    function override_outcomes!(
        outcomes     :: CarerOutcomes,
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes
        if params.yp_contrib_type == no_contribution
                outcomes.housing_cont = 0.0
                outcomes.other_cont = 0.0
        elseif params.yp_contrib_type == flat_rate
                outcomes.other_cont = params.yp_contrib
                outcomes.housing_cont = 0.0
        elseif params.yp_contrib_type == benefits_only
                if yp.benefits == 0.0
                    outcomes.other_cont = 0.0
                    outcomes.housing_cont = 0.0
                end
        end
        if params.contrib_hb == no_contribution
            outcomes.housing_cont = 0.0
        elseif params.contrib_hb == all_people
            outcomes.housing_cont = yp.housing_costs
        elseif params.contrib_hb == benefits_only
            if yp.benefits != 0.0
                outcomes.housing_cont = yp.housing_costs
            end
        end
        if params.payment == min_payment
            reg = get_pay_class_from_region_code( council_data.rcode )
            minp = getAllowances( reg, year ).age_16_17
            outcomes.allowance = max( outcomes.allowance, minp )
        end
        fsize = size(params.fee)[1]
        if fsize > 0
            @assert fsize == 5
            fee = params.fee[carer.skill_level]
            outcomes.fee = max( fee, outcomes.fee )
        end
        tsize = size(params.taper)[1]
        if tsize > 0
            @assert tsize == 3
            p = yp.age - 17
            outcomes.allowance *= params.taper[p]
            outcomes.fee *= params.taper[p]
            outcomes.housing_cont *= params.taper[p]
            outcomes.other_cont *= params.taper[p]
        end
        outcomes
    end

    function do_one_calc(
        ccode        :: AbstractString,
        year         :: Integer,
        carer        :: Carer,
        yp           :: YP,
        council_data :: DataFrameRow,
        params       :: Params ) :: CarerOutcomes# now override

        if ccode in keys( FUNCS )
            outcomes = FUNCS[ccode](
                ccode,
                year,
                carer,
                yp,
                council_data,
                params )
        else
            outcomes = do_basic_calc(
                ccode,
                year,
                carer,
                yp,
                council_data,
                params )
        end
        override_outcomes!(
            outcomes,
            ccode,
            year,
            carer,
            yp,
            council_data,
            params )
        fillcomponents!( outcomes )
        outcomes
    end # do_one_calc


    function trackseries(
        base    :: Real,
        series  :: Vector,
        base_period :: Integer = 1,
        base_year   :: Integer = AFC_SURVEY_YEAR
        ) :: Vector

        b = series[base_period]
        y = base_year
        n = size( series )[1]
        out = zeros(n)
        for i in 1:n
            x = base * series[i]/b
            out[i]=uprate( x, y, base_year )
            y += 1
        end
        out
    end



end # module Staying Put Sim
