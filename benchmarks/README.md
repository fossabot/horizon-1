# Benchmark de Performance: Horizon vs Apache Kafka

Este diretório contém ferramentas para medir e comparar performance de produção entre Horizon e Apache Kafka.

## Uso Rápido

```powershell
# Comparar Horizon vs Kafka (Horizon deve estar rodando em localhost:9092)
.\run-benchmark.ps1 -Target compare

# Apenas Kafka
.\run-benchmark.ps1 -Target kafka

# Apenas Horizon
.\run-benchmark.ps1 -Target horizon

# Customizar quantidade de mensagens
.\run-benchmark.ps1 -Target compare -MessageCount 500000

# Customizar tamanho da mensagem
.\run-benchmark.ps1 -Target compare -MessageSize 4096
```

## Pré-requisitos

1. **Docker Desktop** rodando
2. **Horizon** rodando em `localhost:9092` com `advertised-host` configurado para Docker

```powershell
# Iniciar Horizon para benchmark com Docker (em outro terminal)
cd ..
.\dist\horizon-windows-amd64.exe -advertised-host host.docker.internal
```

> **Importante**: O flag `-advertised-host host.docker.internal` é necessário porque o container Docker precisa
> se conectar ao host Windows. Sem isso, o benchmark não consegue conectar ao broker.

## Métricas Coletadas

- **Throughput**: mensagens/segundo, MB/segundo
- **Latência**: P50, P90, P95, P99, P99.9, Max

## Configuração do Benchmark

O benchmark usa a ferramenta `kafka-producer-perf-test` (nativa do Kafka) com as seguintes configurações:

| Parâmetro | Valor | Descrição |
|-----------|-------|-----------|
| `batch.size` | 1MB | Tamanho do batch de produção |
| `linger.ms` | 10ms | Tempo de espera para batch |
| `acks` | all | Aguarda confirmação de todas réplicas |
| `buffer.memory` | 128MB | Buffer de memória do producer |
| `partitions` | 8 | Número de partições do tópico |

## Resultados

Os resultados são salvos em `./results/`:
- `kafka-producer-benchmark.txt` - Resultados do Kafka
- `horizon-producer-benchmark.txt` - Resultados do Horizon

### Exemplo de Saída

```
100000 records sent, 125000.0 records/sec (122.07 MB/sec), 5.2 ms avg latency, 120.0 ms max latency, 4 ms 50th, 8 ms 95th, 15 ms 99th, 45 ms 99.9th.
```

## Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `run-benchmark.ps1` | Script principal de benchmark |
| `docker-compose-benchmark.yml` | Configuração Docker para Kafka e benchmarks |
| `results/` | Diretório com resultados |

## Interpretação dos Resultados

- **records/sec**: Throughput em mensagens por segundo
- **MB/sec**: Throughput em megabytes por segundo
- **avg latency**: Latência média em milissegundos
- **max latency**: Latência máxima observada
- **50th, 95th, 99th, 99.9th**: Percentis de latência
