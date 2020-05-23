function parse_allocation( filename :: String ) :: Dict
  n = 0
  key = "x"
  out=Dict{String,Tuple}()
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
      out[key]=(oldv,newv)
    end;
    print("$key\n");
  end # loop
  out
end # parse_allocation
