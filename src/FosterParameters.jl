module FosterParameters

    using GlobalDecls
    using CareData: CPIINDEX

    export getdefaultparams, Params, Minimum_Allowances, AllowancesDict, makeAllowances, getAllowances
    export DEFAULT_UPRATE_FACTOR

    export ContributionType, actual_contribution, no_contribution, benefits_only, flat_rate, all_people
    export PaymentType, actual_payment, min_payment, age_16_17
    export UpratingType, no_uprating, by_benefits, by_cpi

    mutable struct Minimum_Allowances
        babies :: Real
        pre_primary :: Real
        primary :: Real
        age_11_15 :: Real
        age_16_17 :: Real
    end

    function Base.:*( a::Minimum_Allowances, x::Real )
        a.babies *= x
        a.pre_primary *= x
        a.primary *= x
        a.age_11_15 *= x
        a.age_16_17 *= x
        a
    end

    AllowancesDict = Dict{Region,Minimum_Allowances}

    function makeAllowances( year :: Integer ) :: AllowancesDict
        d = AllowancesDict()
        if year <= 2016 # don't really need to calculate further back
            d[london] = Minimum_Allowances( 142.0, 145.0, 163.0, 184.0, 216.0 )
            d[se] = Minimum_Allowances( 136.0, 140.0, 156.0, 177.0, 208.0 )
            d[rest_of_england] = Minimum_Allowances( 123.0, 126.0, 139.0, 159.0, 185.0 )
        elseif year == 2017
                d[london] = Minimum_Allowances( 144.0, 147.0, 165.0, 187.0, 216.0 )
                d[se] = Minimum_Allowances( 138.0, 142.0, 158.0, 179.0, 211.0 )
                d[rest_of_england] = Minimum_Allowances( 125.0, 128.0, 141.0, 161.0, 188.0 )
        elseif year == 2018
            d[london] = Minimum_Allowances( 146.0, 149.0, 168.0, 190.0, 222.0 )
            d[se] = Minimum_Allowances( 140.0, 144.0, 160.0, 182.0, 214.0 )
            d[rest_of_england] = Minimum_Allowances( 127.0, 130.0, 143.0, 164.0, 191.0 )
        elseif year == 2019
            d[london] = Minimum_Allowances( 149.0, 152.0, 171.0, 193.0, 226.0 )
            d[se] = Minimum_Allowances( 143.0, 147.0, 163.0, 185.0, 218.0 )
            d[rest_of_england] = Minimum_Allowances( 129.0, 132.0, 146.0, 164.0, 194.0 )
        elseif year >= 2020
            d[london] = Minimum_Allowances( 149.0, 152.0, 171.0, 193.0, 226.0 )* CPIINDEX[year]
            d[se] = Minimum_Allowances( 143.0, 147.0, 163.0, 185.0, 218.0 )* CPIINDEX[year]
            d[rest_of_england] = Minimum_Allowances( 129.0, 132.0, 146.0, 164.0, 194.0 )*CPIINDEX[year]
        end
        d
    end

    DEFAULT_UPRATE_FACTOR =
        makeAllowances( 2019 )[london].age_16_17/
        makeAllowances( 2018 )[london].age_16_17


    @enum ContributionType actual_contribution no_contribution  benefits_only flat_rate all_people
    @enum PaymentType actual_payment min_payment age_16_17
    @enum UpratingType no_uprating by_benefits by_cpi

    mutable struct Params
        name            :: AbstractString
        yp_contrib_type :: ContributionType
        contrib_hb      :: ContributionType
        yp_contrib      :: Real
        payment         :: PaymentType
        uprating        :: UpratingType
        taper           :: Array{Real}
        fee             :: Array{Real}
    end

    function getdefaultparams()
        Params(
            "default parameters",
            actual_contribution,
            actual_contribution,
            0.0,
            actual_payment,
            no_uprating,
            [],
            [] )
    end

    function getAllowances( reg :: Region, year :: Integer ) :: Minimum_Allowances
        # println( "getAllowances; reg=$reg year=$year")
        return makeAllowances( year )[reg]
    end

end
