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

    export makeediteddatasets, addonscodesto!,makeditedhbdata

    """
    This is the LA payment spreadsheet from [here]()
    """
    PRE_PAYMENT_DATA = CSV.File(
            DATADIR*"Staying_Put_grant_letter_to_LAs_2019-20_220319.csv",
            delim=',' ) |> DataFrame

    """
    This is my augmented version of the OFSTED fostering spreadsheet from [here](), with added info
    from the [AFC Fostering Study Spreadsheet]().
    """
    PRE_OFDATA =  CSV.File(
        DATADIR*"Fostering_in_England_2017-18_dataset_transposed_extended.csv",
        delim=',',
        missingstrings=["-999","",".."],
        types=maketypeblock(3:1000)
         ) |> DataFrame



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
           end



    """
    add ONS codes for the LA and enclosing region to some dataframe with lanames
    * `cname` - fieldname with LA names in it
    * `ccode` - name for the added ONS LA code
    * `rcode` - name for ONS region code field

    returns - list of names that have not been mapped
    """
    function add_ons_codes_to!( data :: DataFrame, cname :: Symbol, ccode :: Symbol, rcode :: Symbol )
        println( data[cname] )
        data[ccode]=map( c->codefromname(c), data[cname])
        data[rcode]=map( c->regioncodefromname(c), data[cname])
    end


    function make_edited_datasets()
        path = DATADIR*"edited/"
        addonscodesto!( PRE_PAYMENT_DATA, :council, :ccode, :rcode )
        CSV.write( path*"PAYMENT_DATA.csv", PRE_PAYMENT_DATA )
        addonscodesto!( PRE_OFDATA, :council, :ccode, :rcode )
        CSV.write( path*"OFDATA.csv", PRE_OFDATA )
    end

    function make_edited_hb_data( year )
        # just for reference ..
        if year == 2019
            hbdata =  load( DATADIR*"2019-20_LHA_TABLES.csv" ) |> DataFrame
            addonscodesto!( hbdata, :BRMA, :ccode, :rcode )
            CSV.write( DATADIR*"edited/"*"LHA_2019.csv", hbdata )
        elseif year == 2020
            hbdata =  load( DATADIR*"202021_LHA_TABLES.csv" ) |> DataFrame
            addonscodesto!( hbdata, :BRMA, :ccode, :rcode )
            CSV.write( DATADIR*"edited/"*"LHA_2020.csv", hbdata )
        end
    end

end # DataEditing
