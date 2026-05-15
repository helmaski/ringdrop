Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (scoop bucket list | Select-String 'rikettsie')) {
    scoop bucket add rikettsie https://github.com/rikettsie/scoop-bucket
}
if (scoop list | Select-String 'rdrop') {
    scoop update rdrop
} else {
    scoop install rdrop
}