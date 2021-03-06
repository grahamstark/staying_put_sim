#  Modelling The Staying Put System - A Note

## INTRODUCTION

 This brief note describes how we modelled proposed changes to the payment system to the Staying Put scheme, which allows (..)

## WHAT WE MODELLED

The Data and Modelling Strategy sections below discusses how our model was built. Here, we'll discuss how to interpret the results.

The options paper asked us to produce estimates for three schemes (and some variants):

1. the current scheme, projected forwards for five years;
2. a proposed comprehensive scheme covering the costs of staying put with both an allowance and a fee[^FNFEE].
3. a similar scheme to (3) but with a taper - lower total payments (not just fees) to carers after the first year of the scheme.

In the schemes 2 and 3, the Allowance is set so very carer is to be paid at least the minimum recommended by Department for Education for a 16 or 17-year-old (so £226 a week for carers based in London; £218 a week for the South East; and £194 a week for the rest of England)[@dfe_foster_2019]. There is no Government guidance on the fee part but there is some evidence in the Fostering Network survey discussed below on how they are implemented in practice; we model a fee based on the our understanding of the practice in Oldham: a there, fee payments are based on five skill levels, and for Staying Put  a full fee uis paid in the first year of the placement, 50% in the second, and none in the third.

With the exception of the taper in option (3) the reformed systems are modelled as minimum standards; where it appears that the existing systems are more generous for some local authority (LA), that system is retained.

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

All numbers are in 'nominal terms' - projected actual cash payments, with fees, allowances and other amounts increased each year in line with forecast Consumer Price Index (CPI) inflation using Office for Budget Responsibility (OBR) projections [@obr_obr_2019]. We could easily strip out the effects of inflation to produce 'real terms' increases.

How we produced these is discussed in the next section.
## How We Modelled the Staying Put Scheme

In this section we discuss our model of the Staying Put scheme. We discuss:

* the available data, and how we worked round data limitations;
* our modelling strategy; and
* the assumptions we had to make and the effects of varying those assumptions.   

### Data Sources

We used three main sources of data on the existing Staying Put scheme, and on fostering in England more generally:

1. The OFSTED "Fostering in England" Series[@ofsted_fostering_2019]. OFSTED are responsible for the monitoring of the foster care system in England. OFSTED publish local-authority level information on the performance of foster care services (both Local Authority and private). We use their for some baseline figures for numbers of carers and young people in the system, and we we use OFSTED data to make crude imputations of (e.g) foster carer skill levels;
1. The Department For Education (DFE) "Looked-after children" series [@dfe_statistics:_2019], [@department_for_education_looked-after_2018]. In particular we use the "Underlying Data" series which has the most detail available on numbers on the staying put scheme by local authority and age. We use this for entry and exit rates from the scheme, and optionally for some baseline numbers of young people on the scheme, since the DFE numbers appear not to be always consistent with the OFSTED numbers;
1. The Fostering Network's 2017 Survey of local authorities in England [@fostering_network_foster_2017]. We were given access to the underlying database of this survey, which we use for information on how each English council implements payments for the Staying Put scheme.

In addition we've used data on:

* grants to local authorities [@dfe_staying_2019];
* local housing allowance levels [@valuation_office_agency_local_2019];
* minimum wage rates[@hmrc_national_2019];
* benefit rates [@dwp_benefit_2019];
* projected inflation [@obr_obr_2019];
* recommended minimum foster allowances [@dfe_foster_2019]

These are discussed below.

Getting all these data sources into a consistent format was not straightforward. The three main sources were at times inconsistent with each other.  The Fostering Network data, which is crucial given that it is our only disaggregated source on foster payments, required a lot of interpretation before it could be used. There were also a number of essentially minor but nonetheless time-consuming issues merging local-authority level sources which used different names and codings.

### Modelling Strategy

We were tasked with providing local-authority and national- level five year forecasts for the costs of the staying put scheme under various possible systems of payment. We built a computer simulation model for this, which we've published on the GitHub code sharing site [@stark_simple_2019].

Our modelling strategy was:

* we first constructed a synthetic dataset showing our estimate of the numbers on the staying put scheme by age, and the number of carers by skill level. Each carer and young person was modelled as an individual. The constructed data is based on information from the OFSTED and DFE datasets on the numbers in each local authority in care, and the flows into and out of the staying put scheme. Note that this modelled population can differ from the actual recorded numbers on the staying put scheme, especially, since the numbers in the scheme in each local authority are small and vary randomly from year to year (some years may have an unusually large or small cohort of 18-year-olds, for instance).

* we then wrote code which applied rules for payments to each the carer and from the young person in our synthetic datasests; these rules were either our interpretation of the actual rules in place in 2018 from the Fostering Network survey, or the proposed Action for Children reforms.

By comparing the the modelled Fostering Network payments with the reformed payments for each young person and carer, we could then produce a large amount of output on the gross and net costs of each scheme, at the local authority, regional or national level.

We need to make a lot of assumptions for this to work. We discuss some of these assumptions below. Often in modelling work the best strategy is to give a variety of results for different assumptions, and to futher account for uncertainty by randomly perturbing the model in various ways and showing average results, as well as the range of possible results. We return to this below.

The model is written in the Julia programming language [@bezanson_julia:_2017]. Julia is designed to be easily read by non-specialists so it should in principle be possible to refer directly to the source code.

#### Modelling the population of carers and young people

We start with the total numbers of young people in care in each local authority, of all ages.

For this, we need two things: the number of young people who reach 18 in foster care, and then the numbers of those 18 year olds who opt for the Staying Put scheme.

 We've experimented with two strategies for estimating the numbers of young people in care reaching 18.

 1. taking the total numbers in care in each local authority, of all ages, and applying to that an England wide rate for reaching 18 taken from the average of the last 5 year's OFSTED data. The advantage of this is that smooths our LA level populations out somewhat compared to actual data and thus might perform  better in a multi-year forecast;
 2. alternatively we could simply take the reported numbers of young people reaching 18, as reported in either the DFE[^DFE18] or OFSTED data[^OFSTED18]. Curiously, the OFSTED and DFE numbers are different from each other, with the DFE numbers typically being higher.

 Our main reported results are based in the DFE underlying data, on the grounds that the figures produced using this source seem slightly more compatible with the central government grant determinations [@dfe_staying_2019] - the other approaches produce more cases where the grant to a local authority exceeds our modelled cost of the scheme, and it's hard to imagine central Government doing that very often. But model estimates based on the other sources are available (usually a bit lower)

 Not all young people go on to the staying put scheme, and many go on for only a year or two; we therefore apply local authority level rates for joining and staying in the scheme from the DFE data. In this way our synthetic population of young people are 'aged' through the system for 3 years with a proportion dropping out each year. Each year, each young person is randomly assigned to work, education, or 'Not in Education, Employment or Training' (NEET) according to frequencies taken from the DFE dataset. For the NEETs only, Housing Benefit (HB) is assigned based on the latest category A Local Housing Allowance[@valuation_office_agency_local_2019][@fenton_broad_2012][^FNLHA], and also the £57.90 in Income Support/Job Seeker's Allowance (JSA)[@dwp_benefit_2019]. For those imputed to be in work or training, a national minimum wage if 6.15per hour is imputed. Currently, no further imputations are made for those assumed to be in education.

For carers, in absence of any other information, we assume one carer per young person Staying Put. Some payment schemes have fees that vary with skill levels; in leu of anything better we impute skills in 5 levels taken from OFSTED data on the numbers of carers approved for different types of care, at the national level, and then randomly assign carers to these levels.

#### Modelling the payment regimes

As discussed, the main source of information on how Staying Put carers are actually paid is the raw data from the Fostering Network 2018 survey. The information in that document needed quite a lot of interpretation: whether young people are making a contribution to costs and whether there is a fee payment (a single payment to carers independent of the number of children cared for) in addition to the per-young person allowance. In out modelling of the current system we assume no fee and no contribution unless explicitly mentioned in the spreadsheet; it would be interesting to make fees and contributions the default assumption. For most councils, we make a simple basic payment calculation based on the reported allowance levels, and we a fuller calculation for those councils which report more detail [^JCODE1].

Reformed systems are modelled by overriding parts of our modelled actual system: forcing contributions from young people, minimum allowances and so on.

### Key Assumptions

Amongst the key assumptions we make are:

* in our modelling of the actual system, contributions from young people are only made when the FN dataset explicitly mentions that a council enforces this;
* for young people imputed to be in further education, no further modelling is done (for example of grants, housing costs, etc);
* housing costs are modelled only for NEET imputed young people, unless the FC spreasheet explicitly states otherwise;
* money values are generally uprated using OBR forecasts the CPI index [@obr_obr_2019] (the average increase in this is slightly over the 2% mentioned in the AFC spec);
* wages for young people imputed to be in work are based on a 40 hour week at the National Minimum Wage for young people;
* other than the JSA and HB discussed above, we make no further calculations of benefit entitlements or tax/ni payments, for either carers or young people. This is likely most important for the carers rather than the young people, as some may be eligible for tax credits, and the tax system for carers has some interesting wrinkles [@dfe_foster_2019][@dfe_staying_2013]. For young people in staying put, a 40 hours per week job paying the  young person's minimum wage would earn less than the tax allowance.

Further, we don't model any other indirect costs of the scheme, such as additional social workers, or administration and recruitment costs.

With the possible exception of additional tax and benefit modelling, we could straightforwardly re-run the analysis with different assumptions for all these things.

As mentioned, we are also making a number of random assignments in our modelled population (employment status, skills of carers and so on). To smooth out the effects of these, we run the simulations multiple times [^FN200] with different random draws, and report the average of these. Multiple simulations also allows us to estimate the variability in our results as a result of randomness; these ranges are not reported in the results supplied to AfC, but can easily be supplied; they can be very large for individual LAs, though there is of course less variation in the all-England results. These random variations are of course not the only source of uncertainty here - there are also all the modelling assumptions we've detailed above.

# Bibliography

[^FNLHA]: the rental areas used here don't usually coincide with local authorities; the rent used is chosen randomly from those mapped to that local authority. See Fenton (2012) for a Local Authority to BRMA mapping; since Fenton is rather dated, on occassion a national average category A rent had to be used when no mapping was obvious.

[^JCODE1]: The code for this is in the Julia source file [StayingPutSim.jl](https://github.com/grahamstark/staying_put_sim/blob/master/src/StayingPutSim.jl)

[^OFSTED18]: OFSTED (2019), "Number of young people in foster care who became 18"

[^DFE18]: DFE 2019b, Underling Data, File "CareLeavers17182018_amended.csv", column "CL_Stayput_18"

[^FN200]: 200 iterations in the reported results

[^FNFEE]: an allowance is a per-child amount, a fee a per-carer amount; since we assume 1 child per carer on the staying put scheme there is no real difference in our modelling between the two.
