## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new release.

This is re-upload following comments from the CRAN team.
* back ticks in the Description have been replaced with undirected single-quotes

* An example for grapes-.-grapes.Rd was wrapped in \dontrun and is now commented out.
  For context: this example is present to highlight that %.% is designed to intentionally give an error in some cases.

* Previously, the package would download vignette and testing data to the package directory. This is now changed to download to tempdir() by default and only optionally use the package directory to store these files.
