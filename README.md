# Radiant Explorer

[![Next.js](https://img.shields.io/badge/Next.js-16-black?style=flat-square&logo=next.js)](https://nextjs.org/)
[![React](https://img.shields.io/badge/React-19-61DAFB?style=flat-square&logo=react)](https://react.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?style=flat-square&logo=typescript)](https://www.typescriptlang.org/)
[![Rust](https://img.shields.io/badge/Rust-2024-DEA584?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Tailwind CSS](https://img.shields.io/badge/Tailwind-4-06B6D4?style=flat-square&logo=tailwindcss)](https://tailwindcss.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat-square)](LICENSE)

A blockchain explorer for the Radiant network, engineered for efficiency and scalability. It combines a Rust backend for high-throughput block synchronization with a Next.js frontend for the user interface.

### Technical Overview

*   **Syncer Backend**: Implemented in **Rust** using **Tokio** for asynchronous event processing. Handles block indexing with reduced memory overhead.
*   **Frontend Architecture**: Built on **Next.js 16** (App Router) and **React 19**, leveraging Server Components for optimized rendering.
*   **Analytics**: Data visualization for network statistics, including difficulty and hashrate, utilizing **Recharts**.
*   **User Interface**: Responsive layout constructed with **Tailwind CSS**, including system-aware theme support.
*   **Data Persistence**: **PostgreSQL** database managed via **SQLx** for the syncer and **Prisma** for frontend queries.

![radiant explorer home](radiant-explorer-home.png)

---

## Table of Contents

1. [Architecture Overview](#-architecture-overview)
2. [Tech Stack](#-tech-stack)
3. [Services](#-services)
4. [Getting Started](#-getting-started)
5. [Configuration](#%EF%B8%8F-configuration)
6. [Development](#-development)
7. [Project Structure](#-project-structure)
8. [Contributing](#-contributing)
9. [License](#-license)

---

## Architecture Overview

![radiant architecture](radiant_architecture.png)

The explorer follows a microservices architecture with four main components communicating through Docker's internal network.

---

## Tech Stack

### Core

| Component | Technology | Version |
|-----------|------------|---------|
| Frontend Framework | Next.js (App Router) | 16.x |
| UI Library | React | 19.x |
| Language | TypeScript | 5.x |
| Styling | Tailwind CSS | 4.x |
| State Management | TanStack Query | Latest |

### Backend & Data

| Component | Technology | Version |
|-----------|------------|---------|
| Syncer Runtime | Rust | 2024 Edition |
| Async Runtime | Tokio | Latest |
| Database Driver | SQLx | Latest |
| Database | PostgreSQL | 18.1 |
| ORM (Frontend) | Prisma | Latest |

### Infrastructure

| Component | Technology | Purpose |
|-----------|------------|---------|
| Containerization | Docker Compose | Service orchestration |
| Blockchain Node | Radiant Core | Network connectivity |
| Node Base | Ubuntu 22.04 | Node container OS |

---

## Services

### Frontend (`radiant-frontend`)

Web interface for blockchain data visualization and interaction.

- **Framework**: Next.js 16 (App Directory structure)
- **Features**:
  - Block exploration with pagination support
  - Transaction inspection and search functionality
  - Network statistics dashboard
  - Asynchronous state management via React Query
- **UI Components**:
  - Icons provided by Lucide React & React Icons
  - Data visualization using Recharts
  - Conditional class utility via clsx

### Syncer (`radiant-syncer`)

Rust-based service responsible for blockchain synchronization and indexing.

- **Architecture**: Asynchronous execution using the Tokio runtime
- **Database**: Direct PostgreSQL interaction via SQLx with compile-time query verification
- **Performance**:
  - Memory usage: ~500MB (efficient resource management)
  - Zero-copy deserialization implemented where applicable
  - Database connection pooling
- **Communication**: JSON-RPC interface to the Radiant Node

### Database (`radiant-postgres`)

PostgreSQL instance for indexed blockchain data persistence.

- **Engine**: PostgreSQL 18.1 (Alpine Linux variant)
- **Schema Management**: Controlled via Prisma migrations
- **Indexed Data**:
  - Block data (height, hash, timestamp, transaction count)
  - Transaction records (txid, block reference, inputs/outputs)
  - Address ledgers and balance tracking

### Node (`radiant-node`)

Radiant blockchain daemon instance.

- **Source**: [Radiant-Core/Radiant-Core](https://github.com/Radiant-Core/Radiant-Core)
- **Build**: Custom Docker container based on Ubuntu 22.04
- **Ports**:
  - `7332`: JSON-RPC interface exposure

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) >= 24.0
- [Docker Compose](https://docs.docker.com/compose/) >= 2.20
- 8GB+ RAM recommended
- 50GB+ disk space for blockchain data

### Quick Start

```bash
# Clone the repository
git clone https://github.com/your-org/radiant-explorer.git
cd radiant-explorer

# Build and start all services
docker compose up --build -d

# View logs
docker compose logs -f
```

### Access Points

| Service | URL | Description |
|---------|-----|-------------|
| Explorer UI | http://localhost:3333 | Web interface |
| PostgreSQL | localhost:5432 | Database (internal) |
| Node RPC | localhost:7332 | Blockchain RPC |

---

## Configuration

### Environment Variables

#### Syncer Configuration

```env
# RPC Connection
RPC_HOST=node
RPC_PORT=7332
RPC_USER=radiantuser
RPC_PASS=radiantpassword

# Database
DATABASE_URL=postgres://radiant:radiant123@postgres:5432/radiantdb

# Logging
RUST_LOG=info,sqlx=warn,reqwest=warn
```

#### Frontend Configuration

```env
# Database (Prisma)
DATABASE_URL=postgres://radiant:radiant123@postgres:5432/radiantdb

# API Configuration
NEXT_PUBLIC_API_URL=http://localhost:3333/api
```

#### Node Configuration

The Radiant node is configured via `docker-compose.yml` arguments natively:

```yaml
    command:
      - "-server=1"
      - "-printtoconsole"
      - "-rpcuser=radiantuser"
      - "-rpcpassword=radiantpassword"
      - "-rpcbind=0.0.0.0"
      - "-rpcallowip=0.0.0.0/0"
```

### Resource Limits

Default Docker resource configuration:

| Service | Memory Limit | CPU Limit |
|---------|--------------|-----------|
| Frontend | 1GB | - |
| Syncer | 2GB | - |
| PostgreSQL | 2GB | - |
| Node | 4GB | - |

---

## Development

### Stopping Services

```bash
docker compose down
```

### Rebuilding a Single Service

```bash
docker compose up --build -d <service-name>
```

### Viewing Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f radiant-explorer-syncer
```

### Database Access

```bash
docker compose exec radiant-explorer-postgres psql -U radiant -d radiantdb
```

---

## Project Structure

```
radiant-explorer/
├── frontend/                 # Next.js application
│   ├── app/                  # App Router pages
│   ├── components/           # React components
│   ├── lib/                  # Utilities & helpers
│   └── prisma/               # Database schema
├── syncer/                   # Rust syncer service
│   ├── src/
│   │   ├── main.rs           # Entry point
│   │   ├── rpc/              # RPC client
│   │   └── db/               # Database operations
│   └── Cargo.toml
├── node/                     # Radiant node Dockerfile
├── docker-compose.yml        # Service orchestration
└── README.md
```

---

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
