module Utils

        using DataValues

        export make_type_block, counter, basiccensor, averagegain, zeroormissing, assignrand

        function Vec( n :: Integer, initial :: Real ) :: Vector
            zeros(n) .+ initial
        end

        function make_type_block( r::UnitRange, thistype::DataType=Int) :: Dict{Int64,DataType}
                d = Dict{Int64, DataType}()
                for i in r
                        d[i]=thistype
                end;
                d
        end

        function assignrand( props :: Vector, payments :: Vector ) :: Real
                l = length( payments )
                @assert length( props ) == l
                r = rand()
                for j in 1:l
                    println( "j=$j rand=$r props[$j]")
                    if r <= props[j]
                        return payments[j]
                    end
                end
                @assert "black" == "white"
        end


        function Base.strip( m :: Missing )
        end

        function counter( v, what :: Real = 0.0 )::Integer
                c = 0
                for x in v
                        if x > what
                                c += 1
                        end
                end
                c
        end

        """
        returns the string converted to a form suitable to be used as (e.g.) a Symbol,
        with leading/trailing blanks removed, forced to lowercase, and with various
        characters replaced with '_' (at most '_' in a run).
        """
        function basiccensor( s :: AbstractString ) :: AbstractString
                s = strip( lowercase( s ))
                s = replace( s, r"[ \-,\t–]" => "_" )
                s = replace( s, r"[=\:\)\('’‘]" => "" )
                s = replace( s,  r"[\";:\.\?\*”“]" => "" )
                s = replace( s,  r"_$"=> "" )
                s = replace( s,  r"^_"=> "" )
                s = replace( s,  r"^_"=> "" )
                s = replace( s,  r"\/"=> "_or_" )
                s = replace( s,  r"\&"=> "_and_" )
                s = replace( s,  r"\+"=> "_plus_" )
                s = replace( s,  r"_\$+$"=> "" )
                if occursin( r"^[\d].*", s )
                        s = string("v_", s ) # leading digit
                end
                s = replace( s,  r"__+"=> "_" )
                s = replace( s, r"^_" => "" )
                s = replace( s, r"_$" => "" )
                return s
        end


        function averagegain( v :: Vector )
            n = 0.0
            c = 0.0
            for x in v
                if x > 0
                    n += 1
                    c += x
                end
            end
            println( "c=$c n=$n")
            c/n
        end


        function zeroormissing( d :: DataValue )
            return (d == NA ) || ( d == 0 )
        end

        function zeroormissing( d :: Real )
            return ( d == 0 )
        end

        function zeroormissing( t :: Tuple )
            d = zeroormissing.( t )
            return any( d->d, d )
        end

end
