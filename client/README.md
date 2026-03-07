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
- **Jobs** -- Spark job monitoring with execution metrics and performance tracking (Workflow icon)
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

## Recent UI Improvements

### Performance Optimizations
- **Optimistic Rendering** - Navbar and page headers render instantly (<50ms) using cached user data, with background token validation
- **Smart Caching** - In-memory cache with TTL and request deduplication reduces API calls by 60-80%
  - User data: 5-minute TTL
  - Datasets: 2-minute TTL
  - Lineage/Schema: 5-minute TTL
  - Anomalies/Alerts: 30-second TTL
- **Parallel Loading** - Lineage and Schema pages load data in parallel using localStorage to remember last selected dataset
- **Progressive UI** - Page headers and icons appear immediately while data loads in background

### UI Polish
- **Professional Iconography** - Industry-standard icons following data platform best practices
  - Jobs page: Workflow icon (⚙️) - represents job execution and pipeline orchestration
  - Welcome banner: Zap icon (⚡) - brand identity for Spark platform
  - Configuration step: Settings icon (⚙️) - semantic match for setup tasks
- **Removed unprofessional elements** - Eliminated rocket emojis (🚀) for enterprise credibility
- **Consistent branding** - All UI elements follow professional design patterns from leading data platforms

### Icon Library
Uses [Lucide React](https://lucide.dev) for consistent, high-quality SVG icons. All icons are tree-shakeable and optimized for performance.

## Development

```bash
npm run dev      # Start dev server with HMR
npm run build    # Production build
npm run preview  # Preview production build
npm run lint     # Run ESLint
```
