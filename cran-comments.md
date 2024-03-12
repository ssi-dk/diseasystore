## R CMD check results

0 errors | 0 warnings | 1 note


## revdepcheck results

We checked 0 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages


## Notes
In the examples for `test_diseasystore()` -- one of the functions introduced in this release -- we have a wrapped the
example in \donttest{} since it takes longer than 5 seconds to run the example (running on a laptop it takes ~ 2 min).
