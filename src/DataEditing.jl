module DataEditing

    using DataFrames
    using CSV
    using DataFramesMeta
    using Query
    # local
    using ONSCodes

    # local - run add_scripts_to_path.jl first

    using GlobalDecls
    using Utils

    export make_edited_datasets, add_ons_codes_to!,make_edited_hb_data, get_ofsted_data
    export create_actual_2020_allocation, parse_2020_allocation

    function parse_2020_allocation( filename :: String ) :: Dict
        n = 0
        key = "x"
        out=Dict{String,NamedTuple}()
        oldv = -1
        newv = -1
        for line in eachline( filename )
            n += 1
            r = (n % 3)
            if r==1
                  key = line
                  if key == "#"
                      break;
                  end
            elseif r == 2
                oldv = parse(Int64, line )
            else
                newv = parse(Int64, line )
                out[key]=(oldv=oldv,newv=newv)
            end;
            print("$key\n");
        end # loop
        out
    end # parse_allocation

    function create_actual_2020_allocation()
        start_year = 2020
        dd = DATADIR*"edited/$(start_year)"
        gfname = "$dd/GRANTS_$(start_year).csv"
        grantdata = CSV.File( gfname ) |> DataFrame
        grantdata[!,:my_estimated_amount] .= 0
        grantdata[!,:my_estimated_amount]= copy(grantdata[!,:amount])
        new_grants = parse_2020_allocation( "$dd/raw_grant_data_tmp.txt" );
        for (k,v) in new_grants
            grantdata[(grantdata.council.==k),:amount] .= v.newv
        end
        CSV.write( gfname, grantdata )
        grantdata
    end


    function yearstr( year :: Integer ) :: String
        ys = "?"
        if year == 2018
            ys = "2017-18"
        elseif year == 2019
            ys = "2018-19"
        end
        @assert ys != "?"
        ys
    end

    """
    FIXME we only actually have 1 set of this data
    """
    function get_payment_data( year :: Integer )::DataFrame
        CSV.File(
            DATADIR*"Staying_Put_grant_letter_to_LAs_2019-20_220319.csv",
            delim=',' ) |> DataFrame
    end

    """
    This is the LA payment spreadsheet from [here]()
    """
    PRE_PAYMENT_DATA = get_payment_data( GlobalDecls.SIMULATION_YEAR )

    function get_ofsted_data( year :: Integer ) :: DataFrame
        ys = yearstr( year )
        CSV.File(
            DATADIR*"Fostering_in_England_$(ys)_dataset_transposed_extended.csv",
            delim=',',
            missingstrings=["-999","",".."],
            types=make_type_block(3:1000,Float64)
             ) |> DataFrame
    end

    """
    This is my augmented version of the OFSTED fostering spreadsheet from [here](), with added info
    from the [AFC Fostering Study Spreadsheet]().
    """
    PRE_OFDATA =  get_ofsted_data( GlobalDecls.SIMULATION_YEAR )

    function fixup_cc_codes()
        n = size(LAMAPPINGS)[1]
        fixes = DataFrame( CSV.File(
           DATADIR*"las/Local_Authority_Districts_April_2015_Names_and_Codes_in_the_United_Kingdom.csv" ))
        for i in 1:n
            mapname = LAMAPPINGS[:name][i];
            mapcode = LAMAPPINGS[:new_gss_code][i]
            if mapname in fixes[:LAD15NM]
               fixrec = fixes[(fixes.LAD15NM.==mapname),:];
               fixcode = fixrec[:LAD15CD][1];
               # println( "fixcode $fixcode mapcode $mapcode ")
               if fixcode != mapcode
                   println( "change $mapcode to $fixcode for $mapname" );LAMAPPINGS[:new_gss_code][i] = fixcode
               end
            end
        end
    end # fixup_cc_codes



    """
    add ONS codes for the LA and enclosing region to some dataframe with lanames
    * `cname` - fieldname with LA names in it
    * `ccode` - name for the added ONS LA code
    * `rcode` - name for ONS region code field

    returns - list of names that have not been mapped
    """
    function add_ons_codes_to!( data :: DataFrame, cname :: Symbol, ccode :: Symbol, rcode :: Symbol )
        println( data[cname] )
        data[ccode]=map( c->code_from_name(c), data[cname])
        data[rcode]=map( c->region_code_from_name(c), data[cname])
    end


    function make_edited_datasets()
        path = DATADIR*"edited/"
        add_ons_codes_to!( PRE_PAYMENT_DATA, :council, :ccode, :rcode )
        CSV.write( path*"PAYMENT_DATA.csv", PRE_PAYMENT_DATA )
        add_ons_codes_to!( PRE_OFDATA, :council, :ccode, :rcode )
        CSV.write( path*"OFDATA.csv", PRE_OFDATA )
    end

    function make_edited_hb_data( year )
        # just for reference ..
        if year == 2019
            hbdata =  load( DATADIR*"2019-20_LHA_TABLES.csv" ) |> DataFrame
            add_ons_codes_to!( hbdata, :BRMA, :ccode, :rcode )
            CSV.write( DATADIR*"edited/"*"LHA_2019.csv", hbdata )
        elseif year == 2020
            hbdata =  load( DATADIR*"202021_LHA_TABLES.csv" ) |> DataFrame
            add_ons_codes_to!( hbdata, :BRMA, :ccode, :rcode )
            CSV.write( DATADIR*"edited/"*"LHA_2020.csv", hbdata )
        end
    end

end # DataEditing
