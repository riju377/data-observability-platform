# Data Observability Platform - Frontend

React dashboard for visualizing data lineage, schema evolution, metrics, and anomalies.

## Quick Start

```bash
cd client
npm install
npm run dev
# UI at http://localhost:3000
```

## Tech Stack

| Library | Version | Purpose |
|---------|---------|---------|
| React | 19 | UI framework |
| ReactFlow | 11 | Lineage DAG visualization |
| Recharts | 3 | Metrics charts and dashboards |
| Vite | 7 | Build tool with HMR |
| React Router | 7 | Client-side routing |

## Pages

- **Dashboard** -- Anomaly summary, recent alerts, system health
- **Lineage** -- Interactive DAG of table dependencies (ReactFlow)
- **Column Lineage** -- Column-level dependency graph with transform types
- **Datasets** -- Browse all tracked datasets with metadata
- **Schema** -- Current schema and version history with diffs
- **Metrics** -- Time-series charts for row counts and sizes (Recharts)
- **Anomalies** -- Detected anomalies with severity and status management
- **Alerts** -- Alert rule configuration and alert history

## Configuration

The frontend expects the API at `http://localhost:8000` by default. Update the API base URL in the service layer if hosting elsewhere.

## Build for Production

```bash
npm run build
# Output in dist/
```

## Development

```bash
npm run dev      # Start dev server with HMR
npm run build    # Production build
npm run preview  # Preview production build
npm run lint     # Run ESLint
```
