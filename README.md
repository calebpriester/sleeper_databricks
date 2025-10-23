# Sleeper Fantasy Football Data Pipeline

A Databricks Delta Live Tables pipeline for analyzing Sleeper dynasty fantasy football leagues, with comprehensive trade analysis and long-term career value tracking.

## Overview

This project ingests data from the Sleeper API and builds a dimensional data model for dynasty fantasy football analysis. The pipeline handles Sleeper's unique multi-season challenges (new league_ids per season) and provides accurate career point attribution for traded players and draft picks.

**What This Pipeline Tracks:**
- **Player ownership**: Complete lifecycle from acquisition to departure (drafts, trades, waivers, drops)
- **Weekly fantasy points**: Historical scoring data across multiple seasons
- **Trade analysis**: Full trade history with career point attribution for all assets
- **Draft pick lifecycle**: From initial trade through realization (player selection) to career performance
- **Cross-season tracking**: Manager and league identity across Sleeper's annual league_id changes

## Key Features

### Dynasty League Support
Dynasty leagues operate across multiple seasons, creating unique data challenges that this pipeline solves:

- **Cross-Season Identity Resolution**: Uses `cluster_key` to track leagues across Sleeper's annual league_id changes
- **Manager Alias Reconciliation**: Handles username changes (e.g., TakeFlightJetUp → akumthekar) with manual alias mapping
- **Cross-Season Ownership Matching**: Tracks player ownership across season boundaries using cluster_key joins

### Trade Analysis
The pipeline's core strength is evaluating dynasty trades with accurate long-term value metrics:

- **Career Point Attribution**: Tracks total fantasy points scored by players from trade date forward, regardless of subsequent roster changes
- **Draft Pick Realization**: Matches traded draft picks to the players actually selected and their complete career performance
- **Re-Traded Pick Handling**: Correctly attributes pick value when picks are traded multiple times before being used
- **True Value Philosophy**: Measures "what each side gave away" not "what they kept" - career points count even after re-trading assets

### Analysis Notebooks

**Worst Trade Analysis** ([worst_trade_reveal.ipynb](sleeper_databricks/src/fun_analysis/worst_trade_reveal.ipynb))
- Identifies most lopsided trades by career value differential
- Shows complete asset breakdown (players + draft picks) with career points
- Visualizes who "won" and "lost" trades in hindsight
- Implements "What did each side give away?" methodology for true value assessment

## Pipeline Structure

The pipeline is organized into three Delta Live Tables (DLT) pipelines that run sequentially:

```
sleeper_databricks/
├── src/
│   ├── ingestion/
│   │   └── ingestion_pipeline_sql.ipynb    # Sleeper API → Bronze tables
│   ├── core/
│   │   └── core_pipeline_sql.ipynb         # Bronze → Core dimensions/facts
│   ├── trades/
│   │   └── trades_pipeline_sql.ipynb       # Core → Trade analysis tables
│   └── fun_analysis/
│       └── worst_trade_reveal.ipynb        # Analysis notebook (not in DLT)
├── resources/                               # Pipeline YAML configs
└── databricks.yml                           # Databricks asset bundle config
```

### Pipeline Execution Order
1. **Ingestion Pipeline**: Fetches raw data from Sleeper API → Bronze layer
2. **Core Pipeline**: Transforms bronze data → Core dimensions and facts
3. **Trades Pipeline**: Joins core tables → Trade analysis aggregations

## Key Data Models

### Core Pipeline ([core_pipeline_sql.ipynb](sleeper_databricks/src/core/core_pipeline_sql.ipynb))

**Dimension Tables:**
- `dim_league_clusters`: League groups tracked across seasons (cluster_key mapping)
- `dim_manager_roster_map`: Manager identity with username alias reconciliation
- `dim_players`: Player dimension with names, positions, team affiliations
- `dim_player_ownership`: Complete ownership lifecycle (acquisition → departure) with cross-season matching

**Fact Tables:**
- `fact_player_week_enriched`: Weekly fantasy points with cluster_key for dynasty analysis
- `fact_transactions`: All roster transactions (trades, waivers, drops) from Sleeper API

### Trades Pipeline ([trades_pipeline_sql.ipynb](sleeper_databricks/src/trades/trades_pipeline_sql.ipynb))

**Asset Extraction:**
- `fact_trade_player_assets`: Players involved in trades (directional: incoming/outgoing per roster)
- `fact_trade_pick_assets`: Draft picks involved in trades with original owner tracking

**Value Attribution:**
- `fact_trade_player_points_multi_season`: Career points for traded players (from trade date forward, all rosters)
- `bridge_trade_pick_to_player`: Maps traded draft picks to drafted players (handles re-traded picks)
- `fact_trade_pick_realization`: Draft pick career points (links picks → players → fantasy points)

**Aggregations:**
- `agg_trade_winners_enriched`: Complete trade impact with winner/loser determination and asset breakdowns

## Technical Architecture

### Delta Live Tables (DLT)
All pipelines use Databricks Delta Live Tables for:
- **Declarative transformations**: SQL-based materialized views with automatic dependency resolution
- **Data quality enforcement**: Expectations on critical fields (non-null cluster_key, valid transaction types)
- **Incremental processing**: Efficient updates when new Sleeper data arrives
- **Lineage tracking**: Automatic data lineage visualization in Databricks UI

### Cross-Season Tracking Design
Sleeper creates new `league_id` values each season, breaking standard joins. This pipeline solves it with:

**Cluster Key Strategy:**
1. Normalize league names → URL-safe `cluster_key` (e.g., "Heck of a Dynasty" → "heck_of_a_dynasty")
2. Group all `league_id` values for same league under one `cluster_key`
3. Add `cluster_key` to all dimension/fact tables requiring cross-season joins
4. Use `cluster_key` instead of `league_id` for ownership matching, draft pick attribution

**Example**: Tracking player ownership across seasons
```sql
-- Without cluster_key (WRONG - ownership breaks at season boundary)
JOIN ON league_id AND player_id

-- With cluster_key (CORRECT - ownership spans seasons)
JOIN ON cluster_key AND player_id
```

### Trade Value Philosophy
The pipeline implements "What did each side give away?" methodology:

- **Traded players**: Count ALL career points from trade date forward, even after re-trading
- **Draft picks**: Attribute full career value of drafted player to the pick, regardless of who used it
- **Rationale**: Measures true asset value given up in trade, not value retained by receiving team

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed and configured
- Access to configure Sleeper API credentials

### Deployment

1. **Configure Sleeper API Access**
   - Add your Sleeper league IDs to the ingestion configuration
   - Set up API rate limiting parameters if needed

2. **Deploy via Databricks CLI**
   ```bash
   databricks bundle deploy
   ```
   Or use the VS Code Databricks extension deployment panel.

3. **Run Pipelines in Order**
   ```bash
   # Run via CLI
   databricks bundle run ingestion_pipeline
   databricks bundle run core_pipeline
   databricks bundle run trades_pipeline

   # Or use Databricks UI: Workflows → Select Pipeline → Run
   ```

### Running Analysis

After pipelines complete, open [worst_trade_reveal.ipynb](sleeper_databricks/src/fun_analysis/worst_trade_reveal.ipynb) in Databricks and run all cells to see THE WORST TRADE OF ALL TIME.

## Data Quality & Validation

### Key Quality Checks
- **Ownership lifecycle**: `is_current_roster` should never be TRUE when `departed_date` is not NULL
- **Draft pick realization**: All past-season picks should eventually have `is_realized = TRUE`
- **Trade asset counts**: Both sides of every trade should have equal total asset counts (players + picks)
- **Career points**: No negative point values; NULL allowed for unrealized picks

### Common Data Issues
- **Missing draft data**: Some early seasons may lack complete draft history
- **Username changes**: Requires manual alias updates in `dim_manager_roster_map` CTE
- **Failed waivers**: Pipeline filters `status='complete'` transactions to prevent false ownership records

## Documentation

### Inline Documentation
All critical tables have comprehensive header blocks documenting:
- Purpose and business context
- Grain (row-level definition)
- Column descriptions with nullability notes
- Key business logic and calculation methods
- Data quality expectations
- Upstream dependencies

See [core_pipeline_sql.ipynb](sleeper_databricks/src/core/core_pipeline_sql.ipynb) and [trades_pipeline_sql.ipynb](sleeper_databricks/src/trades/trades_pipeline_sql.ipynb) for examples.

### External Resources
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Live Tables Guide](https://docs.databricks.com/delta-live-tables/index.html)
- [Sleeper API Documentation](https://docs.sleeper.com/)

## Project Philosophy

This pipeline answers retrospective dynasty fantasy football questions with accurate hindsight analysis:
- "What was the worst trade in league history?"
- "What's the true long-term value of the player we traded away?"
- "Did that 2nd round pick we traded become a fantasy star?"

The data model prioritizes **accurate career value attribution** over current roster tracking. When a manager trades away an asset, we measure the full value they gave up, regardless of what they later did with assets received in return.
