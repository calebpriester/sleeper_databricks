# Sleeper Fantasy Football Data Pipeline

A Databricks-based data pipeline for analyzing Sleeper fantasy football leagues with a focus on dynasty league trade analysis.

## Overview

This project ingests data from the Sleeper API and builds a comprehensive data lakehouse for fantasy football analysis. The pipeline tracks:

- **Player stats**: Weekly fantasy points across multiple seasons
- **Trades**: Complete trade history with player and draft pick assets
- **Draft picks**: Full draft pick lifecycle from trade to realization
- **Rosters**: Manager/roster mappings and league structure

## Key Features

### Dynasty Trade Analysis
The pipeline's core strength is evaluating dynasty trades with long-term career value metrics:

- **Career Point Attribution**: Tracks total fantasy points scored by players from trade date forward
- **Draft Pick Realization**: Links draft picks to the players selected and their career performance
- **Multi-Season Tracking**: Handles Sleeper's per-season league_id changes using normalized cluster_keys
- **True Value Metrics**: Measures asset value regardless of subsequent ownership changes

### Analysis Capabilities

**Worst Trade Analysis** ([worst_trade_reveal.ipynb](sleeper_databricks/src/fun_analysis/worst_trade_reveal.ipynb))
- Identifies most lopsided trades by career value differential
- Shows complete asset breakdown (players + draft picks)
- Visualizes who "won" and "lost" trades in hindsight
- Uses the "What did each side give away?" methodology

## Pipeline Structure

```
sleeper_databricks/
├── src/
│   ├── ingestion/          # Sleeper API data ingestion
│   ├── core/               # Core player/roster data models
│   ├── trades/             # Trade analysis pipeline
│   └── fun_analysis/       # Analysis notebooks
├── resources/              # Pipeline configuration
└── databricks.yml          # Databricks asset bundle config
```

## Key Data Models

### Core Tables
- `dim_players`: Player dimension with names, positions
- `fact_player_week_enriched`: Weekly fantasy points with cluster_key for cross-season joins
- `dim_manager_roster_map`: Manager to roster mappings

### Trade Tables
- `fact_trade_player_assets`: Players involved in trades (incoming/outgoing per roster)
- `fact_trade_pick_assets`: Draft picks involved in trades
- `fact_trade_player_points_multi_season`: Career points for traded players (from trade date forward)
- `fact_trade_pick_realization`: Links draft picks to selected players and their career points
- `agg_trade_winners_enriched`: Aggregated trade impact with winner/loser determination

## Recent Improvements

### Fixed Career Point Attribution (2024-10)
**Problem**: Tyler Lockett showed 0 career points in worst trade analysis despite scoring 600+ points
**Root Cause**: `fact_trade_player_points_multi_season` filtered by roster_id, only counting points while player was on receiving roster
**Solution**: Removed roster_id filter to count ALL career points regardless of ownership changes

This aligns with the analysis goal: "measure what each side gave away" not "what they received before trading again"

## Getting Started

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running Pipelines

The pipelines run in sequence:
1. **Ingestion**: Pull data from Sleeper API
2. **Core**: Build player/roster dimension tables
3. **Trades**: Calculate trade impact metrics

To run a deployed pipeline, hover over it in the **Deployments** panel and click **Run**.

### 3. Running Analysis

Navigate to [src/fun_analysis/worst_trade_reveal.ipynb](sleeper_databricks/src/fun_analysis/worst_trade_reveal.ipynb) and run all cells to see THE WORST TRADE OF ALL TIME.

## Documentation

- [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)

## Project Notes

This pipeline was built to answer dynasty fantasy football questions like:
- "What was the worst trade in league history?"
- "What's the true long-term value of a player we traded away?"
- "Did that draft pick we traded become a star?"

The data model prioritizes answering these retrospective "hindsight is 20/20" questions with accurate career value attribution.
