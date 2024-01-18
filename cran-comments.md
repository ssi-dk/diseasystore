## R CMD check results

0 errors | 0 warnings | 1 note


## revdepcheck results

We checked 0 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages


## Notes
This is a minor release focusing on fixing issues

This update addresses the following issues:

* On 11/01/2024, we were notified of a failing test on r-devel.
  This error has been resurfacing intermittently and we believe we have identified the root cause and are mitigating it in this release.

* In the same mail On 11/01/2024, we were notified that the package did not fail gracefully when internet resources are unavailable.
  This is also being addressed in this update.

* With the release of SCDB v0.3 on 13/01/2024, some tests were failing for this package. This is fixed in this update.