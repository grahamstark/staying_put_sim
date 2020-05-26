module GlobalDecls

    export DATADIR, RESULTSDIR, SKIPLIST, WEEKS_PER_YEAR, annualise
    export Region, london, se, rest_of_england
    export AggLevel, national, regional, local_authority
    export SIMULATION_YEAR

    const DATADIR="/home/graham_s/VirtualWorlds/projects/action_for_children/england/data/"
    const RESULTSDIR="/home/graham_s/VirtualWorlds/projects/action_for_children/england/results/"

    const SIMULATION_YEAR = 2019

    @enum AggLevel national regional local_authority
    @enum Region london se rest_of_england
    WEEKS_PER_YEAR = 365.25/7

    SKIPLIST = [
        # "E07000189" # south somerset missing in underlying data
    ]

    function annualise( m :: Real ) :: Real
        return m*WEEKS_PER_YEAR/1_000_000.0
    end

end
