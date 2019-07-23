## WHAT WE MODELLED

Section [X] below discusses how our model was built. Here, we'll discuss how to interpret the results.

The options paper asked us to produce estimates for three schemes (and some variants):

1. the current scheme, projected forwards for five years;
2. a comprehensive scheme covering the costs of staying put with both an allowance and a fee[^FNFEE]. Every carer was to be paid at least the minimum allowance Government sets to cover the cost of caring for a 16 or 17-year-old (so £226 a week for carers based in London; £218 a week for the South East; and £194 a week for the rest of England)[@dfe_foster_2019]. There is no Government guidance on the fee part but there is some evidence [] on how they are implemented in practice. We model a fee based on the our understanding of the Oldham model: a fee based on five skill levels, with 100% paid in the first year, 50% in the second, and none in the third;
3. a similar scheme but with a taper - lower payments to carers after the first year of the scheme.

The reformed systems are modelled as minimum standards; where it appears that the existing systems are more generous for some local authority (LA), that system is retained.

## Outputs

Our model produces five main classes of output:

* A) Payments to carers;
* B) Gross Costs to LAs;
* C) Contributions from young people;
* D) Forecast Grants from central Government;
* E) Net Cost to local authorities

Formally:

    B = A-C

so, gross costs to LAs is the payments to carers, less contributions from young people (we make no attempt to estimate the overhead costs of social workers, administration, etc.).

    E = A-C-D

the net costs to LAs are the gross costs less grants from Central Government.

All numbers are in nominal terms (money amounts uncorrected for inflation). We use projected Consumer Price Index (CPI) inflation using Office for Budget Responsibility (OBR) projections [@obr_obr_2019].

How we produced these is discussed in the next section.
