<#
.SYNOPSIS
    Executa benchmark de producao comparando Horizon vs Apache Kafka

.DESCRIPTION
    Este script usa kafka-producer-perf-test (ferramenta nativa do Kafka)
    para comparar performance de producao entre Horizon e Apache Kafka.
    
    Metricas coletadas:
    - Throughput (mensagens/segundo, MB/segundo)
    - Latencia (P50, P90, P95, P99, P99.9)

.PARAMETER Target
    Alvo do benchmark: "kafka", "horizon" ou "compare" (ambos)

.PARAMETER MessageSize
    Tamanho da mensagem em bytes (default: 1024)

.PARAMETER MessageCount
    Quantidade de mensagens a enviar (default: 100000)

.EXAMPLE
    .\run-benchmark.ps1 -Target compare
    
.EXAMPLE
    .\run-benchmark.ps1 -Target horizon -MessageCount 500000

.NOTES
    Requisitos:
    - Docker Desktop rodando
    - Horizon rodando em localhost:9092 (se testando horizon)
#>

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("kafka", "horizon", "compare")]
    [string]$Target = "compare",
    
    [Parameter(Mandatory=$false)]
    [int]$MessageSize = 1024,
    
    [Parameter(Mandatory=$false)]
    [int]$MessageCount = 100000
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "================================================================" -ForegroundColor Blue
Write-Host "  Benchmark de Producao: Horizon vs Apache Kafka" -ForegroundColor Blue  
Write-Host "================================================================" -ForegroundColor Blue
Write-Host ""
Write-Host "Configuracao:" -ForegroundColor Cyan
Write-Host "  Target:        $Target"
Write-Host "  Message Size:  $MessageSize bytes"
Write-Host "  Message Count: $MessageCount"
Write-Host ""

# Criar diretorio de resultados
$ResultsDir = Join-Path $ScriptDir "results"
if (-not (Test-Path $ResultsDir)) {
    New-Item -ItemType Directory -Path $ResultsDir | Out-Null
}

# Verificar se Horizon esta rodando (se necessario)
if ($Target -eq "horizon" -or $Target -eq "compare") {
    Write-Host "Verificando Horizon em localhost:9092..." -ForegroundColor Yellow
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.Connect("localhost", 9092)
        $tcpClient.Close()
        Write-Host "[OK] Horizon esta rodando" -ForegroundColor Green
        Write-Host ""
        Write-Host "IMPORTANTE: Certifique-se de iniciar o Horizon com:" -ForegroundColor Yellow
        Write-Host "  .\dist\horizon-windows-amd64.exe -advertised-host host.docker.internal" -ForegroundColor Cyan
        Write-Host ""
    }
    catch {
        Write-Host "[ERRO] Horizon nao esta rodando em localhost:9092" -ForegroundColor Red
        Write-Host ""
        Write-Host "Inicie o Horizon primeiro (com advertised-host para Docker):" -ForegroundColor Yellow
        Write-Host "  .\dist\horizon-windows-amd64.exe -advertised-host host.docker.internal" -ForegroundColor Gray
        Write-Host ""
        exit 1
    }
}

# Mover para diretorio do script
Push-Location $ScriptDir

try {
    # Definir variaveis de ambiente para docker-compose
    $env:NUM_RECORDS = "$MessageCount"
    $env:RECORD_SIZE = "$MessageSize"
    
    switch ($Target) {
        "kafka" {
            Write-Host ""
            Write-Host "--- Executando benchmark contra Apache Kafka ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml up -d kafka
            Write-Host "Aguardando Kafka iniciar..." -ForegroundColor Yellow
            Start-Sleep -Seconds 30
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-kafka
            docker-compose -f docker-compose-benchmark.yml down -v
        }
        
        "horizon" {
            Write-Host ""
            Write-Host "--- Executando benchmark contra Horizon ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-horizon
        }
        
        "compare" {
            # Primeiro Kafka
            Write-Host ""
            Write-Host "--- Executando benchmark contra Apache Kafka ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml up -d kafka
            Write-Host "Aguardando Kafka iniciar..." -ForegroundColor Yellow
            Start-Sleep -Seconds 30
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-kafka
            docker-compose -f docker-compose-benchmark.yml down -v
            
            # Depois Horizon
            Write-Host ""
            Write-Host "--- Executando benchmark contra Horizon ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-horizon
            
            # Mostrar comparacao
            Write-Host ""
            Write-Host "================================================================" -ForegroundColor Magenta
            Write-Host "  Comparacao de Resultados" -ForegroundColor Magenta
            Write-Host "================================================================" -ForegroundColor Magenta
            Write-Host ""
            
            if (Test-Path "$ResultsDir\kafka-producer-benchmark.txt") {
                Write-Host "KAFKA:" -ForegroundColor Yellow
                Get-Content "$ResultsDir\kafka-producer-benchmark.txt" | Select-Object -Last 5
                Write-Host ""
            }
            
            if (Test-Path "$ResultsDir\horizon-producer-benchmark.txt") {
                Write-Host "HORIZON:" -ForegroundColor Yellow
                Get-Content "$ResultsDir\horizon-producer-benchmark.txt" | Select-Object -Last 5
                Write-Host ""
            }
            
            Write-Host "Resultados completos em: $ResultsDir" -ForegroundColor Green
        }
    }
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Benchmark concluido!" -ForegroundColor Green
Write-Host ""
