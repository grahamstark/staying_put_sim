by_la_sys_iteration_and_year = main_results |>
    @filter( _.year > 2018 && _.year < 2025 ) |>
    @groupby( [_.ccode,  _.year, _.sysno, _.iteration ] ) |>
    @map({
        year=first(_.year),
        ccode=first(_.ccode),
        sysno=first(_.sysno),
        iteration=first(_.iteration),
        cnt=length( _ ),
        income=sum( _.income_recieved )*52.0,
        contribs = sum( _.contributions_from_yp )*52.0,
        payments = sum( _.payments_from_la )*52.0
        }

        ) |>
    @groupby( [_.ccode,  _.year, _.sysno] ) |>
    @map(
            {
               year   = first(_.year),
               ccode  = first(_.ccode),
               sysno  = first(_.sysno),

               avg_income=mean( _.income  ),
               min_income=minimum( _.income ),
               max_income=maximum( _.income ),
               pct_10_income=quantile( _.income, [0.10] )[1],
               pct_25_income=quantile( _.income, [0.25] )[1],
               pct_75_income=quantile( _.income, [0.75] )[1],
               pct_90_income=quantile( _.income, [0.90] )[1],

               avg_contribs = mean( _.contribs ),
               min_contribs = minimum( _.contribs ),
               max_contribs = maximum( _.contribs ),
               pct_10_contribs=quantile( _.contribs, [0.10] )[1],
               pct_25_contribs=quantile( _.contribs, [0.25] )[1],
               pct_75_contribs=quantile( _.contribs, [0.75] )[1],
               pct_90_contribs=quantile( _.contribs, [0.90] )[1],

               avg_payments = mean( _.payments  ),
               min_payments = minimum( _.payments ),
               max_payments = maximum( _.payments ),
               pct_10_payments=quantile( _.payments, [0.10] )[1],
               pct_25_payments=quantile( _.payments, [0.25] )[1],
               pct_75_payments=quantile( _.payments, [0.75] )[1],
               pct_90_payments=quantile( _.payments, [0.90] )[1]
            }
        ) |>
    @orderby( [_.year,_.ccode,_.sysno] ) |>
    DataFrame
