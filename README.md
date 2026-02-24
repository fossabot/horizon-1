# Horizon - Kafka-Compatible Event Streaming Platform

A high-performance, Kafka protocol-compatible event streaming platform implemented in Go. Inspired by WarpStream and EventHorizon.

## Features

- ✅ **Kafka Protocol Compatible** - Works with existing Kafka clients
- ✅ **Persistent Storage** - Durable log segments with configurable retention
- ✅ **Topic Partitioning** - Horizontal scaling through partitions
- ✅ **Consumer Groups** - Coordinated consumption with rebalancing
- ✅ **High Performance** - Optimized for throughput and low latency
- ✅ **Cross-Platform** - Runs on Windows, Linux, and macOS

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Horizon Broker                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Network   │  │  Protocol   │  │   Consumer Group    │  │
│  │   Server    │──│   Handler   │──│     Coordinator     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                    │              │
│         └────────────────┼────────────────────┘              │
│                          ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    Topic Manager                         │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐              │ │
│  │  │ Topic A  │  │ Topic B  │  │ Topic C  │  ...         │ │
│  │  │ P0 P1 P2 │  │ P0 P1    │  │ P0       │              │ │
│  │  └──────────┘  └──────────┘  └──────────┘              │ │
│  └─────────────────────────────────────────────────────────┘ │
│                          │                                   │
│                          ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                   Storage Engine                         │ │
│  │  ┌─────────────────────────────────────────────────────┐ │
│  │  │              Log Segments (Append-Only)             │ │
│  │  │  segment-000000.log  segment-001000.log  ...        │ │
│  │  └─────────────────────────────────────────────────────┘ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Requisitos

- **Go 1.22+** (para build local)
- **Docker** e **Docker Compose** (opcional, para build e execução em container)
- **Make** (Linux/macOS) ou **PowerShell** (Windows)

## Build

### Build Rápido (Plataforma Atual)

**Windows (PowerShell):**
```powershell
# Build simples
.\scripts\build.ps1

# Build com versão específica
.\scripts\build.ps1 -Target build -Version 1.0.0
```

**Linux/macOS (Make):**
```bash
# Build simples
make build

# Ou diretamente com Go
go build -o horizon ./cmd/horizon
```

### Build para Múltiplas Plataformas

O projeto suporta cross-compilation para Linux, Windows e macOS (amd64 e arm64).

**Windows (PowerShell):**
```powershell
# Todas as plataformas
.\scripts\build.ps1 -Target build-all

# Plataformas específicas
.\scripts\build.ps1 -Target build-linux
.\scripts\build.ps1 -Target build-windows
.\scripts\build.ps1 -Target build-darwin
```

**Linux/macOS (Make):**
```bash
# Todas as plataformas
make build-all

# Plataformas específicas
make build-linux
make build-windows
make build-darwin
```

Os binários são gerados no diretório `dist/`:
```
dist/
├── horizon-linux-amd64
├── horizon-linux-arm64
├── horizon-windows-amd64.exe
├── horizon-windows-arm64.exe
├── horizon-darwin-amd64
└── horizon-darwin-arm64
```

### Build com Docker (Sem Go Instalado)

Se você não tem Go instalado, pode usar Docker para compilar:

```powershell
# Windows
.\scripts\build.ps1 -Target docker-build

# Linux/macOS
make docker-build
```

Isso usa o container `golang:1.22-alpine` para compilar os binários para todas as plataformas.

## Execução

### Execução Local

**Windows:**
```powershell
# Com configuração padrão
.\horizon.exe

# Com arquivo de configuração
.\horizon.exe -config configs/config.yaml

# Com opções inline
.\horizon.exe -port 9092 -data-dir .\data
```

**Linux/macOS:**
```bash
# Com configuração padrão
./horizon

# Com arquivo de configuração
./horizon -config configs/config.yaml

# Com opções inline
./horizon -port 9092 -data-dir ./data
```

### Execução com Docker

**Iniciar o container:**
```powershell
# Windows
.\scripts\build.ps1 -Target docker-run

# Linux/macOS
make docker-run

# Ou diretamente com docker-compose
docker-compose -f deployments/docker-compose.yml up -d horizon
```

**Parar o container:**
```bash
docker-compose -f deployments/docker-compose.yml down
```

**Build e execução da imagem Docker:**
```powershell
# Build da imagem
docker build -f build/Dockerfile -t horizon:latest .

# Executar
docker run -d -p 9092:9092 -v horizon-data:/data --name horizon horizon:latest
```

### Variáveis de Ambiente

O Horizon suporta configuração via variáveis de ambiente:

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `HORIZON_BROKER_ID` | ID do broker | `1` |
| `HORIZON_HOST` | Host de bind | `0.0.0.0` |
| `HORIZON_PORT` | Porta do broker | `9092` |

### Testes

```powershell
# Windows
.\scripts\build.ps1 -Target test

# Linux/macOS
make test

# Com cobertura
make test-coverage
```

### Limpeza

```powershell
# Windows
.\scripts\build.ps1 -Target clean

# Linux/macOS
make clean
```

## Configuração

Edite o arquivo `configs/config.yaml`:

```yaml
broker:
  id: 1
  host: "0.0.0.0"
  port: 9092
  cluster_id: "horizon-cluster"

storage:
  data_dir: "./data"
  segment_size_mb: 1024
  retention_hours: 168
  sync_writes: false

defaults:
  num_partitions: 3
  replication_factor: 1

performance:
  write_buffer_kb: 2048
  max_connections: 10000
  io_threads: 4
```

## Kafka Client Compatibility

Horizon is compatible with standard Kafka clients. Example using `kafka-go`:

```go
package main

import (
    "context"
    "github.com/segmentio/kafka-go"
)

func main() {
    // Producer
    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "my-topic",
    })
    
    writer.WriteMessages(context.Background(),
        kafka.Message{Value: []byte("Hello, Horizon!")},
    )
    
    // Consumer
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "my-topic",
        GroupID: "my-group",
    })
    
    msg, _ := reader.ReadMessage(context.Background())
    println(string(msg.Value))
}
```

## Supported Kafka APIs

| API | Version | Status |
|-----|---------|--------|
| ApiVersions | 0-3 | ✅ |
| Metadata | 0-12 | ✅ |
| Produce | 0-9 | ✅ |
| Fetch | 0-13 | ✅ |
| ListOffsets | 0-7 | ✅ |
| FindCoordinator | 0-4 | ✅ |
| JoinGroup | 0-7 | ✅ |
| SyncGroup | 0-5 | ✅ |
| Heartbeat | 0-4 | ✅ |
| LeaveGroup | 0-4 | ✅ |
| OffsetCommit | 0-8 | ✅ |
| OffsetFetch | 0-8 | ✅ |
| CreateTopics | 0-7 | ✅ |
| DeleteTopics | 0-6 | ✅ |
| DescribeGroups | 0-5 | ✅ |
| ListGroups | 0-4 | ✅ |

## Project Structure

```
horizon/
├── cmd/
│   └── horizon/          # Main application entry point
├── internal/
│   ├── broker/           # Broker logic & consumer groups
│   ├── config/           # Configuration loading
│   ├── protocol/         # Kafka wire protocol (reader/writer)
│   ├── server/           # TCP server & request handlers
│   └── storage/          # Log storage engine (segments, partitions)
├── build/
│   └── Dockerfile        # Container image build
├── configs/
│   └── config.yaml       # Default configuration
├── deployments/
│   ├── docker-compose.yml              # Main deployment
│   └── docker-compose-kafka-test.yml   # Kafka compatibility test
├── scripts/
│   ├── build.ps1                       # Windows build script
│   └── test-kafka-compatibility.ps1    # Binary compat test
├── benchmarks/
│   ├── docker-compose-benchmark.yml    # Benchmark environment
│   ├── run-benchmark.ps1               # Benchmark runner
│   └── results/                        # Benchmark outputs
├── Makefile              # Linux/macOS build targets
├── go.mod                # Go module
└── README.md
```

## Benchmarks

```
BenchmarkProduce-8         500000    2341 ns/op    1.7 GB/s
BenchmarkFetch-8           800000    1523 ns/op    2.6 GB/s
BenchmarkPartitionWrite-8  1000000   1102 ns/op    3.6 GB/s
```

## License

MIT License - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) first.
