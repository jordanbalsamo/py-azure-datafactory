

$psScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

cd $psScriptPath
cd ../src

$adfScriptName = 'executeAdf.py'
pwd

$n = 1

$uB = 10

While($n -le $uB){

    Write-Host "`nPowerShell: Starting test run [$n] of [$uB]..." -ForegroundColor Green
    python $adfScriptName
    Write-Host "`nPowerShell: Finished test run [$n] of [$uB]" -ForegroundColor Yellow

    Start-Sleep  -s 1

    $n++

}


