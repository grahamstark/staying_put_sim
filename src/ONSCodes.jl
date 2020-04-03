
module ONSCodes

    using Query
    using CSV
    using DataFrames

    using GlobalDecls
    using Utils

    export load_las, region_name_from_code, region_name_from_name, region_code_from_code, region_code_from_ccode
    export region_code_from_name, code_from_name, is_aggregate, create_brma_lookup, pick_brma_at_random

    """
    This loads the file of ONS codes and names I downloaded from [here]().
    """
    function load_las( name :: AbstractString ) :: DataFrame
        df = CSV.File(
            name,
            delim='\t' ) |> DataFrame
        lcnames = Symbol.(lowercase.(basiccensor.(string.(names(df)))))
        rename!(df,lcnames)
        for n in names(df)
            df[!,n]=map( n->strip(n), df[!,n])
        end
        df
    end

    """
    Broad Rental Market Areas (BRMA)
    """
    function create_brma_lookup()
        las = load_las( DATADIR*"las/all_las.tab" )
        brmap = CSV.File( DATADIR*"las/brmas/ladistrict_2_brma.csv" ) |> DataFrame
        lcnames = Symbol.(lowercase.(basiccensor.(string.(names(brmap)))))
        rename!(brmap,lcnames)
        for n in names(brmap)
            if typeof( brmap[n])==String
                brmap[n]=map( n->strip(n), brmap[n])
            end
        end
        println( brmap )
        brlook = make_brma_lookup(0)
        nlas = size(las)[1]
        for r in 1:nlas
            la = las[r,:]
            which = brmap.la .== la.old_ons_code
            brlas = brmap[which,:]
            ccode = la.new_gss_code
            n = size( brlas )[1]
            println( la.new_gss_code )
            println( brlas )
            println( n )
            out = [ccode, n, missing, missing, missing, missing, missing, missing, missing, missing, missing, missing ]
            for k in 1:n
                brla = brlas[k,:]
                out[k+2] = basiccensor(brla.brmaname)
            end
            println( out )
            push!( brlook, out )
        end
        # FIXME general load of this sort
        brvalues = CSV.File( DATADIR*"las/brmas/2019-20_LHA_TABLES.csv" ) |> DataFrame
        lcnames = Symbol.(lowercase.(basiccensor.(string.(names(brvalues)))))
        rename!(brvalues,lcnames)
        brvalues[:brma]=map( n->basiccensor(n), brvalues[:brma])

        CSV.write( DATADIR*"las/brmas/2019-20_LHA_TABLES_EDITED.csv", brvalues )
        CSV.write( DATADIR*"las/brmas/brma_lookup.csv", brlook )

        (brlook=brlook,brvalues=brvalues)
    end

    LAMAPPINGS = load_las( DATADIR*"las/all_las.tab" )
    BRVALUES_2019 = CSV.File( DATADIR*"las/brmas/2019-20_LHA_TABLES_EDITED.csv" ) |> DataFrame
    BRVALUES_2020 = CSV.File( DATADIR*"las/brmas/2020-21_LHA_TABLES_EDITED.csv" ) |> DataFrame
    BRVALUES = BRVALUES_2020
    BRLOOKUP = CSV.File( DATADIR*"las/brmas/brma_lookup.csv" ) |> DataFrame

    function make_brma_lookup( n :: Integer ) :: DataFrame
        DataFrame(
            ccode     = Vector{AbstractString}(undef,n),
            n         = Vector{Integer}(undef,n),
            brma1     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma2     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma3     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma4     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma5     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma6     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma7     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma8     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma9     = Vector{Union{AbstractString,Missing}}(missing,n),
            brma10     = Vector{Union{AbstractString,Missing}}(missing,n))
    end

    function pick_brma_at_random( ccode :: AbstractString ) :: AbstractString
        which =BRLOOKUP.ccode .== ccode
        row = BRLOOKUP[which,:]
        # println(typeof(row))
        if size( row )[1] > 0
            n = row[1,:].n
            if n > 0
                r = rand(1:n)
                brs = Symbol("brma$r")
                return row[1,brs]
            end
        end
        return ""
    end

    function get_brma_value( ccode :: AbstractString, field :: Symbol = :cat_a ) :: Real
        name = pick_brma_at_random( ccode )
        v = -1.0
        if( name != "")
            which = BRVALUES.brma .== name
            vv = BRVALUES[which,field]
            if length( vv )[1] > 0
                v = vv[1]
            else
                println( "getbrvalue failed for ccode=|$ccode| name=|$name|")
            end
        end
        v
    end

    function region_name_from_name( name :: AbstractString )
           q = @from i in LAMAPPINGS begin
               @where (i.name == name )
               @select lad = i.region
           end
           s=missing
           for i in q
               s = i
           end
           s
    end

    function region_name_from_code( code :: AbstractString )
           q = @from i in LAMAPPINGS begin
               @where (i.new_gss_code == code )
               @select lad = i.region
           end
           s=missing
           for i in q
               s = i
           end
           s
    end

    function region_code_from_code( name :: AbstractString )
        region = region_name_from_code( name )
        return code_from_name( region )
    end

    function region_code_from_name( name :: AbstractString )
        region = region_name_from_name( name )
        return code_from_name( region )
    end

    function code_from_name( name :: Missing )
        missing
    end

    function code_from_name( name :: Nothing )
        missing
    end

    """
    returns the ONS code for the given LA/Region name
    """
    function code_from_name( name :: AbstractString )
        # println( "name $name " )
       q = @from i in LAMAPPINGS begin
           @where basiccensor(i.name) == basiccensor(name)
           @select lad = i.new_gss_code
       end
       s=missing
       for i in q
           s = i
       end
       s
    end

    """
    Another hack - faster lookup for ccode->region since the Query version
    is paralyising things.
    """
    function make_region_lookup( LAS :: DataFrame ) :: Dict
        d = Dict()
        for i in eachrow( LAS )
            d[i.new_gss_code] = region_code_from_code( i.new_gss_code )
        end
        d
    end

    CCODE_TO_RCODE_LOOKUP = make_region_lookup( LAMAPPINGS )

    """
    faster version of below, using a simple Dict
    """
    function region_code_from_ccode( ccode :: AbstractString ) :: AbstractString
        return CCODE_TO_RCODE_LOOKUP[ccode]
    end

    function is_aggregate( ccode :: Union{Missing,AbstractString} )
        ctype = typeof( ccode )
        println( "is_aggregate; ccode $ccode type $ctype " )
        if ccode === missing
            return true
        end
        if length( ccode ) <= 1 || ccode=="NA" # different ways of loading a dataframe produce either missing or 0 length string
            return true
        end
        (ccode[1:3] in ["E92","E12"])
    end

end # LAMappings
