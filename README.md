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
│   ├── core/                                # Bronze → Core dimensions/facts
│   │   ├── dimensions.ipynb                #   - League clusters, managers, ownership, drafts
│   │   ├── facts.ipynb                     #   - Player weekly stats, team performance
│   │   └── aggregates.ipynb                #   - Career totals, consistency, H2H records
│   ├── trades/                              # Core → Trade analysis tables
│   │   ├── trades_dimensions.ipynb         #   - Trade metadata, staging, completeness
│   │   ├── trades_facts.ipynb              #   - Player/pick assets, career attribution
│   │   └── trades_aggregates.ipynb         #   - Trade impact, winners, enriched views
│   ├── marts/
│   │   └── marts_pipeline_sql.ipynb        # Business-ready reporting views
│   └── fun_analysis/
│       └── worst_trade_reveal.ipynb        # Analysis notebook (not in DLT)
├── resources/                               # Pipeline YAML configs
└── databricks.yml                           # Databricks asset bundle config
```

### Pipeline Execution Order
1. **Ingestion Pipeline**: Fetches raw data from Sleeper API → Bronze layer (`sleeper_raw` schema)
2. **Core Pipeline**: Transforms bronze data → Core dimensions/facts/aggregates (`sleeper_core` schema)
   - Organized as modular notebooks (dimensions → facts → aggregates)
3. **Trades Pipeline**: Joins core tables → Trade analysis (`sleeper_trades` schema)
   - Depends on core pipeline completion via `pipelines.dependencies` configuration
4. **Marts Pipeline**: Business-ready views for dashboards and reports (`sleeper_marts` schema)

## Key Data Models

### Core Pipeline

**Dimensions** ([dimensions.ipynb](sleeper_databricks/src/core/dimensions.ipynb))
- `dim_league_clusters`: League groups tracked across seasons (cluster_key mapping)
- `dim_manager_roster_map`: Manager identity with username alias reconciliation
- `dim_player_ownership`: Complete ownership lifecycle (acquisition → departure) with cross-season matching
- `dim_draft_metadata`: Draft configuration with pre-calculated pick numbers (snake/linear logic)
- `dim_draft_picks`: Complete draft pick information with manager names and drafted players

**Facts** ([facts.ipynb](sleeper_databricks/src/core/facts.ipynb))
- `fact_team_week`: Weekly team performance (points for/against, wins/losses)
- `fact_player_week`: Weekly player fantasy points with starter/bench tracking
- `fact_player_week_enriched`: Player weekly stats with cluster_key for dynasty analysis
- `fact_standings_week`: Historical weekly standings and playoff positions
- `fact_waiver_acquisitions`: Waiver wire activity with FAAB tracking

**Aggregates** ([aggregates.ipynb](sleeper_databricks/src/core/aggregates.ipynb))
- `agg_consistency_metrics`: Team scoring variability (stddev, coefficient of variation)
- `agg_rivalry_head_to_head`: Manager vs manager all-time records
- `agg_records_all_time`: League-wide high/low scoring records
- `agg_player_roster_totals`: Player career totals by roster
- `agg_draft_roi_by_round`: Draft pick value analysis by round

### Trades Pipeline

**Dimensions** ([trades_dimensions.ipynb](sleeper_databricks/src/trades/trades_dimensions.ipynb))
- `stg_trade_transactions`: Staging table for completed trades with league context
- `dim_trade_metadata`: Trade metadata with league type and cluster information
- `dim_trade_completeness`: Tracks whether all draft picks in a trade have been realized

**Facts** ([trades_facts.ipynb](sleeper_databricks/src/trades/trades_facts.ipynb))
- `fact_trade_player_assets`: Players involved in trades (directional: incoming/outgoing per roster)
- `fact_trade_player_points_multi_season`: Career points for traded players (from trade date forward)
- `fact_trade_pick_assets`: Draft picks involved in trades with original owner tracking
- `bridge_trade_pick_to_player`: Maps traded draft picks to drafted players (handles re-traded picks)
- `fact_trade_pick_points_career`: Career points for players drafted with traded picks
- `bridge_trade_pick_unique`: Deduplicated view of traded picks (one row per unique pick)

**Aggregates** ([trades_aggregates.ipynb](sleeper_databricks/src/trades/trades_aggregates.ipynb))
- `agg_trade_impact_by_horizon`: Trade value by time horizon (same season, 1yr, 2yr, career)
- `agg_trade_impact_summary`: Trade impact with multiple time horizons aggregated
- `agg_trade_winners`: Head-to-head trade comparison with winner determination
- `agg_trade_winners_enriched`: Trade winners with manager names

## Technical Architecture

### Modular Pipeline Design
The core and trades pipelines are organized into modular notebooks following dimensional modeling best practices:

**Benefits:**
- **Maintainability**: Smaller, focused notebooks are easier to understand and modify
- **Reusability**: Core dimensions (like `dim_draft_metadata`) centralize complex logic once
- **Parallel Execution**: Databricks can potentially execute independent notebooks concurrently
- **Clear Dependencies**: Dimensions → Facts → Aggregates progression within each pipeline
- **Consistency**: Both core and trades follow the same organizational pattern

**Dependency Chain:**
```
Raw (Bronze) → Core Dimensions → Core Facts → Core Aggregates
                      ↓
              Trades Dimensions → Trades Facts → Trades Aggregates
```

### Delta Live Tables (DLT)
All pipelines use Databricks Delta Live Tables for:
- **Declarative transformations**: SQL-based materialized views with automatic dependency resolution
- **Data quality enforcement**: Expectations on critical fields (non-null cluster_key, valid transaction types)
- **Incremental processing**: Efficient updates when new Sleeper data arrives
- **Lineage tracking**: Automatic data lineage visualization in Databricks UI
- **Cross-pipeline dependencies**: Trades pipeline waits for core completion via `pipelines.dependencies`

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

See core pipeline notebooks ([dimensions.ipynb](sleeper_databricks/src/core/dimensions.ipynb), [facts.ipynb](sleeper_databricks/src/core/facts.ipynb), [aggregates.ipynb](sleeper_databricks/src/core/aggregates.ipynb)) and trades pipeline notebooks for examples.

### Memory Files
Project-specific documentation and findings are stored in [.claude/](.claude/):
- [databricks_notebook_format.md](.claude/databricks_notebook_format.md): Required structure for programmatically creating Databricks notebooks

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
