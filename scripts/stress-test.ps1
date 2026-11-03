# Netflix Microservice High-Throughput Resilience Stress Test Script
param(
    [string]$BaseUrl = "http://localhost:8080",
    [int]$TotalRequests = 25,
    [int]$Concurrency = 5,
    [int]$FailPercentage = 70,
    [string]$ClientId = "terminal-subscriber-1"
)

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "🎬 Triggering Netflix Resilience Concurrent Load Test..." -ForegroundColor Cyan
Write-Host "Base URL         : $BaseUrl" -ForegroundColor Yellow
Write-Host "Total Requests   : $TotalRequests" -ForegroundColor Yellow
Write-Host "Concurrency      : $Concurrency parallel threads" -ForegroundColor Yellow
Write-Host "Simulated Fail % : $FailPercentage%" -ForegroundColor Yellow
Write-Host "Client ID        : $ClientId" -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Cyan

$endpoint = "$BaseUrl/api/v1/content/simulate-load?totalRequests=$TotalRequests&concurrency=$Concurrency&failPercentage=$FailPercentage&clientId=$ClientId"

try {
    $response = Invoke-RestMethod -Uri $endpoint -Method Post
    Write-Host "`nTest Complete! Summary Results:" -ForegroundColor Green
    $response | Format-List

    Write-Host "`nFetching Live Resilience Health Status..." -ForegroundColor Cyan
    $health = Invoke-RestMethod -Uri "$BaseUrl/api/v1/content/health-status?clientId=$ClientId" -Method Get
    Write-Host "Overall Health : $($health.overallHealth)" -ForegroundColor Green
    Write-Host "Circuit State  : $($health.circuitBreaker.state)" -ForegroundColor Green
    Write-Host "Failure Rate   : $($health.circuitBreaker.currentFailureRate)" -ForegroundColor Green
    Write-Host "Available Tokens: $($health.rateLimiter.availablePermissions)" -ForegroundColor Green
} catch {
    Write-Host "Error executing stress test: $_" -ForegroundColor Red
}

