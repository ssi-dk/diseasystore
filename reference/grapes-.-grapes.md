# Existence aware pick operator

Existence aware pick operator

## Usage

``` r
env %.% field
```

## Arguments

- env:

  (`object`)  
  The object or environment to attempt to pick from

- field:

  (`character`)  
  The name of the field to pick from `env`

## Value

Error if the `field` does not exist in `env`, otherwise it returns
`field`

## Examples

``` r
 t <- list(a = 1, b = 2)

 t$a       # 1
#> [1] 1
 t %.% a   # 1
#> [1] 1

 t$c # NULL
#> NULL
 try(t %.% c) # Gives error since "c" does not exist in "t"
#> Error : c not found in t
```
