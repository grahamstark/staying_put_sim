## How We Modelled the Staying Put Scheme

In this section we discuss our model of staying put. We discuss:

* the available data, and how we worked round limitations in the data;
* our modelling strategy; and
* the assumptions we had to make and the effects of varying those assumptions.
*   

### Data Sources

We used three main sources of data on the existing Staying Put scheme, and on fostering in England more generally:

1. The [@ofsted_fostering_2019] "Fostering in England" Series. OFSTED are resposible for the monitoring of the foster care system in England. They publish local-authority level information on the performance of foster care services (both Local Authority and private). We use their for some baseline numbers of numbers of carers and young people in the system, and we we use OFTSTED data to make crude imputations of (e.g) foster carer skill levels;
1. Department For Education (DFE) "Looked-after children" series [@dfe_statistics:_2019], [@department_for_education_looked-after_2018]. In particular we use the "Underlying Data" series which has the most detail on numbers on the staying put scheme by local authority and age. We use this for entry and exit rates from the scheme, and optionally for some baseline numbers of young people on the scheme, since the DFE numbers appear not to be always consistent with the OFSTED numbers;
1. The Fostering Network's 2017 Survey of local authorities in England [@fostering_network_foster_2017]. We were given access to the underlying dataset. We use this for information on how each English council implements payments for the Staying Put scheme.

In addition we've used data on:

* grants to local authorities [@dfe_staying_2019];
* local housing allowance levels [@valuation_office_agency_local_2019];
* minimum wage rates[@hmrc_national_2019];
* benefit rates [@dwp_benefit_2019];
* projected inflation [@obr_obr_2019];
* recommended minimum foster allowances [@dfe_foster_2019]

These are discussed below.

Getting all these data sources into a consistent format was not always straightforward. The three main sources listed above at times appeared to be inconsistent with each other.  The Fostering Network data, which is crucial given that it is our only disaggregated source on foster payments, required a lot of iterpretation before it could be used. There were also a number of essentially minor but nonetheless time-consuming issues merging local-authority level sources which used different names and codings.

### Modelling Strategy

We were tasked with providing local-authority and national- level five year forecasts for the costs of the staying put scheme under various possible systems of payment. We built a computer simulation model for this, which we've published on the [GitHub code sharing site](https://github.com/grahamstark/staying_put_sim/).

Our modelling strategy was:

* we first constructed a synthetic dataset showing our estimate of the numbers on the staying put scheme by age, and the number of carers by skill level. Each carer and young person was modelled as an individual. The constructed data is based on information from the OFSTED and DFE datasets on the numbers in each local authority in care, and the flows into and out of the staying put scheme. Note that this modelled population can differ from the actual recorded numbers on the staying put scheme, especially, since the numbers in the scheme in each local authority are small and vary randomly from year to year (some years may have an unusually large or small cohort of 18-year-olds, for instance).

* we then wrote code which applied rules for payments to each the carer and from the young person in our synthetic datasests; these rules were either our interpretation of the actual rules in place in 2018 from the Fostering Network survey, or the proposed Action for Children reforms.

By comparing the the modelled Fostering Network payments with the reformed payments for each young person and carer, we could then produce a large amount of output on the gross and net costs of each scheme, at the local authority, regional or national level.

As discussed further below, we need to make a lot of assumptions for this to work. We discuss some of these assumptions below. Often in modelling work the best strategy is to give a variety of results for different assumptions, and to futher account for uncertainty by randomly perturbing the model in various ways and showing average results, as well as the range of possible results.

#### Modelling the population of carers and young people

We start with the total numbers of young people in care in each local authority, of all ages.

In the default settings, we then generate the numbers reaching 18 by applying an England wide rate taken from the average of the last 5 year's OFSTED data. As discussed, this smooths our LA level populations out somewhat compared to actual data. Not all young people go on to the staying put scheme, and many go on for only a year or two; we therefore apply local authority level rates for joining and staying in the scheme from the DFE data. In this way our synthetic population of young people are 'aged' through the system for 3 years with a proportion dropping out each year. Each year, each young person is randomly assigned to work, education, or 'Not in Education, Employment or Training' (NEET) according to frequencies taken from the DFE dataset. For the NEETs only, a housing cost is assigned the latest category A Local Housing Allowance[^FNLHA], and also the £57.90 in income support/JSA. For those imputed to be in work or training, a national minumum wage if 6.15per hour is imputed. Currently, no further imputations are made for those assumed to be in education.

For carers, in absence of any other information, we assume 1 carer per young person Staying Put. Some payment schemes have fees that vary with skill levels; in leu of anything better we impute skills in 5 levels taken from OFSTED data on the numbers of carers approved for different types of care, at the national level, and then randomly assign carers to these levels.

To  


#### Modelling the payment regimes



### Limitations

[@obr_obr_2019]

## Bibliography

[^FNLHA]: the rental areas used here don't usually coincide with local authorities; the rent used is chosen randomly from those mapped to that local authority. See Fenton (2012) for a Local Authority to BRMA mapping; since Fenton is rather dated, on occassion a national average category A rent had to be used when no mapping was obvious.
