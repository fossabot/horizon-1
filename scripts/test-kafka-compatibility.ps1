# test-kafka-compatibility.ps1
# Script para testar compatibilidade binária do Horizon com Kafka
# Descobre automaticamente tópicos e partições a partir da estrutura de diretórios
#
# Uso: .\scripts\test-kafka-compatibility.ps1

param(
    [switch]$Clean,
    [switch]$Help
)

if ($Help) {
    Write-Host @"
Horizon Kafka Binary Compatibility Test
=======================================

Este script importa automaticamente os dados do Horizon para o Kafka,
descobrindo topicos e particoes a partir da estrutura de diretorios.

Estrutura esperada:
    ./data/topico-0/00000000000000000000.log
    ./data/topico-1/00000000000000000000.log
    ./data/outro-topico-0/00000000000000000000.log

Uso:
    .\scripts\test-kafka-compatibility.ps1           # Executar importacao
    .\scripts\test-kafka-compatibility.ps1 -Clean    # Limpar ambiente
    .\scripts\test-kafka-compatibility.ps1 -Help     # Mostrar ajuda

Apos a execucao, acesse http://localhost:8081 para visualizar no Kafka UI.
"@
    exit 0
}

# Root directory (one level up from scripts/)
$RootDir = Split-Path -Parent $PSScriptRoot
if (-not $RootDir) { $RootDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path) }
Push-Location $RootDir

$ErrorActionPreference = "Continue"
$ComposeFile = "deployments/docker-compose-kafka-test.yml"
$DataDir = ".\data"
$ProjectName = (Get-Item .).Name.ToLower() -replace '[^a-z0-9]', ''
$VolumeName = "${ProjectName}_kafka-data"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "  Horizon -> Kafka Import Tool" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

if ($Clean) {
    Write-Host "Limpando ambiente..." -ForegroundColor Yellow
    docker-compose -f $ComposeFile down -v 2>$null
    Write-Host "Ambiente limpo!" -ForegroundColor Green
    exit 0
}

# Descobrir tópicos e partições
Write-Host "Analisando estrutura de dados..." -ForegroundColor Yellow
$partitionDirs = Get-ChildItem -Path $DataDir -Directory -ErrorAction SilentlyContinue

if ($partitionDirs.Count -eq 0) {
    Write-Host "ERRO: Nenhum diretorio encontrado em $DataDir" -ForegroundColor Red
    Write-Host "Primeiro execute o Horizon e produza algumas mensagens." -ForegroundColor Yellow
    exit 1
}

# Agrupar por tópico
$topics = @{}
foreach ($dir in $partitionDirs) {
    $name = $dir.Name
    # Formato: topic-partition (ex: teste-0, meu-topico-1)
    $lastDash = $name.LastIndexOf('-')
    if ($lastDash -gt 0) {
        $topicName = $name.Substring(0, $lastDash)
        $partitionId = $name.Substring($lastDash + 1)
        
        if (-not $topics.ContainsKey($topicName)) {
            $topics[$topicName] = @()
        }
        
        $logFile = Join-Path $dir.FullName "00000000000000000000.log"
        $logSize = 0
        if (Test-Path $logFile) {
            $logSize = (Get-Item $logFile).Length
        }
        
        $topics[$topicName] += @{
            Partition = [int]$partitionId
            Dir = $dir.Name
            LogSize = $logSize
        }
    }
}

if ($topics.Count -eq 0) {
    Write-Host "ERRO: Nenhum topico encontrado em $DataDir" -ForegroundColor Red
    Write-Host "Esperado formato: topico-particao (ex: my-topic-0)" -ForegroundColor Yellow
    exit 1
}

# Mostrar estrutura descoberta
Write-Host ""
Write-Host "Topicos encontrados:" -ForegroundColor Green
foreach ($topic in $topics.Keys | Sort-Object) {
    $partitions = $topics[$topic] | Sort-Object { $_.Partition }
    $partCount = $partitions.Count
    $totalSize = 0
    foreach ($p in $partitions) { $totalSize += $p.LogSize }
    Write-Host "  $topic ($partCount particoes, $totalSize bytes)" -ForegroundColor White
    foreach ($p in $partitions) {
        Write-Host "    - Particao $($p.Partition): $($p.LogSize) bytes" -ForegroundColor Gray
    }
}

# Limpar ambiente anterior
Write-Host ""
Write-Host "Limpando ambiente anterior..." -ForegroundColor Yellow
docker-compose -f $ComposeFile down -v 2>$null

# Subir Kafka (KRaft mode - sem Zookeeper)
Write-Host ""
Write-Host "Subindo Kafka (KRaft mode)..." -ForegroundColor Yellow
docker-compose -f $ComposeFile up -d kafka

# Aguardar Kafka ficar pronto
Write-Host "Aguardando Kafka inicializar..." -ForegroundColor Yellow
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    $null = docker exec kafka-test kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    Write-Host "ERRO: Kafka nao inicializou a tempo" -ForegroundColor Red
    exit 1
}

Write-Host "Kafka pronto!" -ForegroundColor Green

# Criar tópicos
Write-Host ""
Write-Host "Criando topicos no Kafka..." -ForegroundColor Yellow
foreach ($topic in $topics.Keys | Sort-Object) {
    $partCount = $topics[$topic].Count
    docker exec kafka-test kafka-topics --create --topic $topic --partitions $partCount --replication-factor 1 --bootstrap-server kafka:9092 --if-not-exists 2>&1 | Out-Null
    Write-Host "  Criado: $topic ($partCount particoes)" -ForegroundColor Gray
}

# Aguardar partições serem atribuídas
Write-Host ""
Write-Host "Aguardando particoes serem atribuidas..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Parar Kafka para copiar arquivos
Write-Host ""
Write-Host "Parando Kafka para copiar arquivos..." -ForegroundColor Yellow
docker stop kafka-test | Out-Null
Start-Sleep -Seconds 3

# Copiar arquivos de log para cada partição
Write-Host ""
Write-Host "Copiando arquivos do Horizon..." -ForegroundColor Yellow
Write-Host "  Volume: $VolumeName" -ForegroundColor Gray

foreach ($topic in $topics.Keys | Sort-Object) {
    foreach ($p in $topics[$topic]) {
        $srcLog = "/horizon-data/$($p.Dir)/00000000000000000000.log"
        $dstDir = "/kafka-data/$($p.Dir)"
        
        # Copiar arquivo .log
        docker run --rm -v "${PWD}/data:/horizon-data:ro" -v "${VolumeName}:/kafka-data" alpine:latest cp $srcLog $dstDir/ 2>$null
        
        # Remover índices antigos para forçar reconstrução
        docker run --rm -v "${VolumeName}:/kafka-data" alpine:latest rm -f "$dstDir/00000000000000000000.index" "$dstDir/00000000000000000000.timeindex" 2>$null
        
        # Ajustar permissões
        docker run --rm -v "${VolumeName}:/kafka-data" alpine:latest chown 1000:1000 "$dstDir/00000000000000000000.log" 2>$null
        
        Write-Host "  Copiado: $($p.Dir) ($($p.LogSize) bytes)" -ForegroundColor Gray
    }
}

# Iniciar Kafka novamente
Write-Host ""
Write-Host "Iniciando Kafka com dados do Horizon..." -ForegroundColor Yellow
docker start kafka-test | Out-Null

# Aguardar Kafka carregar os dados
Write-Host "Aguardando Kafka carregar os dados..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Verificar se Kafka carregou as mensagens
Write-Host ""
Write-Host "Verificando logs carregados..." -ForegroundColor Yellow
$logOutput = docker logs kafka-test 2>&1 | Select-String -Pattern "logEndOffset" | Select-Object -Last 10
foreach ($line in $logOutput) {
    if ($line -match "logEndOffset=(\d+)") {
        $offset = $matches[1]
        if ([int]$offset -gt 0) {
            Write-Host "  $($line.Line.Trim())" -ForegroundColor Green
        }
    }
}

# Subir Kafka UI
Write-Host ""
Write-Host "Subindo Kafka UI..." -ForegroundColor Yellow
docker-compose -f $ComposeFile up -d kafka-ui-test

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "  Importacao Concluida!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Topicos importados:" -ForegroundColor Cyan
foreach ($topic in $topics.Keys | Sort-Object) {
    $partCount = $topics[$topic].Count
    Write-Host "  - $topic ($partCount particoes)" -ForegroundColor White
}
Write-Host ""
Write-Host "Acesse o Kafka UI em: http://localhost:8081" -ForegroundColor Cyan
Write-Host ""
Write-Host "Para limpar: .\scripts\test-kafka-compatibility.ps1 -Clean" -ForegroundColor Gray

# Restore original directory
Pop-Location
