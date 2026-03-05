# 🏀 NBA Data Analytics — Complete SQL Project

> **A production-grade SQL analytics portfolio project** built on 20 seasons of NBA data (2003–2022).  
> 75 fully validated queries across 9 skill categories, from basic aggregation to advanced window functions, custom basketball metrics, and executive-level reporting.

---

## 📊 Dataset Overview

| Table | Description | Rows |
|---|---|---|
| `fact_games` | One row per game — scores, FG%, 3P%, assists, rebounds | 26,651 |
| `fact_player_game_stats` | Player box score every game | 668,628 |
| `fact_standings` | Daily standings snapshots | 210,342 |
| `dim_teams` | 30 NBA franchises | 30 |
| `dim_players` | All tracked players | 2,687 |
| `dim_date` | Calendar dimension | 4,304 |
| `dim_season` | Season labels (2003–2022) | 20 |

**Total:** ~900,000+ records across 20 seasons (2003–2022)

---

## 🏗️ Star Schema Architecture

```
                   ┌─────────────┐
                   │  DIM_Date   │
                   └──────┬──────┘
                          │
┌──────────────┐    ┌─────▼──────────┐    ┌──────────────┐
│  DIM_Teams   │◄───│  FACT_Games    │───►│  DIM_Season  │
└──────┬───────┘    │  (26,651 rows) │    └──────────────┘
       │            └─────┬──────────┘
       │                  │
       │    ┌─────────────▼──────────────┐
       └────│  FACT_PlayerGameStats      │
            │  (668,628 rows)            │
            └────────────────────────────┘
                          │
                   ┌──────▼──────┐
                   │ DIM_Players │
                   └─────────────┘
```

**Design principles applied:**
- Fact tables contain measurable events (games, player performances)
- Dimension tables contain descriptive attributes (teams, players, dates)
- Pre-aggregated analytics views for dashboard performance
- Foreign key relationships enforced for data integrity

---

## 📁 Project Structure

```
NBA_SQL_Analytics_Project/
│
├── NBA_SQL_Analytics_Project.sql    ← Main project file (75 questions)
├── NBA_DataWarehouse.db             ← SQLite database (star schema)
├── NBA_Analytics_Dashboard.xlsx     ← Excel analytics workbook (5 tabs)
├── PowerBI_DAX_Measures.txt         ← 18 DAX measures for Power BI
├── NBA_PowerBI_Dashboard_Guide.html ← Interactive Power BI setup guide
│
└── PowerBI_CSVs/                    ← 11 clean CSVs for Power BI import
    ├── DIM_Teams.csv
    ├── DIM_Players.csv
    ├── DIM_Date.csv
    ├── DIM_Season.csv
    ├── FACT_Games.csv
    ├── ANALYTICS_TeamSeason.csv
    ├── ANALYTICS_PlayoffProbability.csv
    ├── ANALYTICS_PlayerSeason.csv
    ├── ANALYTICS_HeadToHead.csv
    ├── ANALYTICS_MonthlyTrends.csv
    └── ANALYTICS_FourFactors.csv
```

---

## 🗂️ Question Index (75 SQL Queries)

### Section A — Schema Setup & Data Exploration (Q01–Q05)
| Q# | Question | Skills Used |
|---|---|---|
| Q01 | What tables exist? How many rows each? | System tables, UNION ALL |
| Q02 | Check for NULL values in fact_games | NULL handling, data profiling |
| Q03 | Date range and season coverage | DATE functions, aggregation |
| Q04 | Games played per season | GROUP BY, ORDER BY, COUNT |
| Q05 | Full team directory with arena info | SELECT, CAST, ORDER BY |

### Section B — Team Performance Analysis (Q06–Q15)
| Q# | Question | Skills Used |
|---|---|---|
| Q06 | Top 10 teams by all-time wins | Multi-table JOIN, GROUP BY |
| Q07 | Classify teams into performance tiers | CASE WHEN classification |
| Q08 | Home court advantage by team | CTE, derived metrics |
| Q09 | Points scored avg/max/min per team | Multiple aggregates, STDEV |
| Q10 | Teams with 55+ win seasons (multiple) | CTE, HAVING, sustained excellence |
| Q11 | Teams above league average scoring | Subquery in WHERE clause |
| Q12 | Segment teams by eFG% bracket | CASE WHEN + GROUP_CONCAT |
| Q13 | Team performance with arena details | Multi-table JOIN |
| Q14 | Top 20 highest scoring games ever | Expression columns, JOIN |
| Q15 | Blowout win/loss records by team | CTE, CASE in aggregation |

### Section C — Player Statistics & Rankings (Q16–Q26)
| Q# | Question | Skills Used |
|---|---|---|
| Q16 | Top 20 scorers 2022-23 season | Multi-metric aggregation |
| Q17 | All-time scoring leaders | Full dataset GROUP BY |
| Q18 | Players who averaged a triple-double | CTE, multi-condition filter |
| Q19 | Most triple-double games (single game) | CASE WHEN inside SUM |
| Q20 | Most 40+ point game performances | COUNT with filter |
| Q21 | Double-double season leaders | CTE filtering |
| Q22 | Top 2 scorers on every team | ROW_NUMBER per partition |
| Q23 | Efficiency by position (G/F/C) | GROUP BY non-numeric |
| Q24 | Best plus/minus average | Multi-filter aggregation |
| Q25 | True Shooting % formula | Formula-derived metric |
| Q26 | Most improved scorers (2019→2022) | CTE self-join on season |

### Section D — Window Functions & Rankings (Q27–Q35)
| Q# | Question | Skills Used |
|---|---|---|
| Q27 | RANK vs DENSE_RANK vs ROW_NUMBER | All three rank functions |
| Q28 | Year-over-year win change | LAG() and LEAD() |
| Q29 | Cumulative wins over time | Running SUM() OVER |
| Q30 | 3-season rolling average win% | ROWS BETWEEN moving average |
| Q31 | Scoring quartile segmentation | NTILE(4) |
| Q32 | Best/worst season per franchise | FIRST_VALUE, MIN/MAX OVER |
| Q33 | Win% percentile ranking | PERCENT_RANK, CUME_DIST |
| Q34 | Each team's #1 scorer per year | RANK PARTITION BY two columns |
| Q35 | Most consistent scorers (lowest StDev) | Manual STDEV via window |

### Section E — Advanced Metrics (Q36–Q43)
| Q# | Question | Skills Used |
|---|---|---|
| Q36 | Calculate eFG% from raw data | Formula derivation |
| Q37 | Full Four Factors analysis | Multi-metric comparison |
| Q38 | Best Net Rating team each season | CTE + RANK by year |
| Q39 | Pace vs scoring correlation | Multi-season aggregation |
| Q40 | Player Efficiency Rating (PER approx) | Per-36 normalization formula |
| Q41 | Box Plus/Minus approximation | Weighted composite formula |
| Q42 | Scoring breakdown: 3PT vs 2PT vs FT | CASE-based decomposition |
| Q43 | Win Shares approximation + luck factor | Derived formula + CASE |

### Section F — Time Series & Trend Analysis (Q44–Q50)
| Q# | Question | Skills Used |
|---|---|---|
| Q44 | League-wide scoring trend over 20 seasons | LAG + GROUP BY season |
| Q45 | Monthly scoring patterns | strftime date extraction |
| Q46 | Golden State Warriors dynasty analysis | Era labeling with CASE |
| Q47 | 3-point shooting evolution 20 years | LAG for YoY change |
| Q48 | Classify each season's playing style | CASE on aggregated metrics |
| Q49 | Monthly performance breakdown per team | Date grouping multi-metric |
| Q50 | Best 10-game stretch in a season | ROW_NUMBER rolling window |

### Section G — Head-to-Head & Matchup Analysis (Q51–Q56)
| Q# | Question | Skills Used |
|---|---|---|
| Q51 | All-time BOS vs LAL record | CASE-based win counting |
| Q52 | Rivalry win% matrix (top 5 teams) | Self-join, GROUP BY |
| Q53 | Highest-scoring team matchups | Composite key JOIN |
| Q54 | Closest games ever (1-2pt margin) | Extreme value filtering |
| Q55 | Clutch record — games decided by ≤5 pts | CTE + complex aggregation |
| Q56 | Best player in BOS vs MIL matchups | Filtered player-game join |

### Section H — Playoff & Standings Analysis (Q57–Q63)
| Q# | Question | Skills Used |
|---|---|---|
| Q57 | Full 2022-23 playoff picture | Multi-level CASE classification |
| Q58 | Most consistently elite franchises | Window RANK + COUNT |
| Q59 | Biggest single-season turnarounds | LAG + dramatic trend |
| Q60 | Championship odds by conference seed | GROUP BY on rank column |
| Q61 | Biggest franchise collapses/rises | LAG + ABS change |
| Q62 | Tiered playoff probability model | CASE probability logic |
| Q63 | East vs West comparison | GROUP BY conference + season |

### Section I — Business & Interview Questions (Q64–Q75)
| Q# | Question | Skills Used |
|---|---|---|
| Q64 | Find the Nth highest scorer (N=5) | LIMIT/OFFSET + subquery |
| Q65 | Detect duplicate game records | GROUP BY + HAVING > 1 |
| Q66 | Home court advantage trend | YoY % calculation |
| Q67 | Calculate median points (no built-in) | Median in SQL (hard!) |
| Q68 | Outlier game performances (z-score) | Statistical z-score in SQL |
| Q69 | Undervalued free agent targets | Multi-metric ratio analysis |
| Q70 | Pivot: team wins by year as columns | CASE-based PIVOT pattern |
| Q71 | Teams with better away than home record | CTE + anomaly detection |
| Q72 | Player loyalty/retention analysis | 3-way self-join on seasons |
| Q73 | RFM-style player segmentation | RFM framework in SQL |
| Q74 | Chained CTE: team strength vs weakness | 4-step CTE pipeline |
| Q75 | Executive summary — complete snapshot | Multi-CTE cross-join report |

---

## ⚡ SQL Skills Demonstrated

```
Basic SQL         │ SELECT · WHERE · GROUP BY · ORDER BY · HAVING · DISTINCT
Joins             │ INNER JOIN · LEFT JOIN · SELF JOIN · CROSS JOIN
Aggregations      │ COUNT · SUM · AVG · MIN · MAX · Manual STDEV
Subqueries        │ Correlated · In WHERE · In FROM · In SELECT
CTEs              │ Single CTE · Chained CTEs · Recursive pattern
CASE WHEN         │ Classification · Pivot · Conditional aggregation
Window Functions  │ RANK · DENSE_RANK · ROW_NUMBER · NTILE
                  │ LAG · LEAD · FIRST_VALUE · LAST_VALUE
                  │ SUM OVER · AVG OVER (Running & Rolling)
                  │ PERCENT_RANK · CUME_DIST
Date Functions    │ strftime · date arithmetic · month/year extraction
NULL Handling     │ IS NULL · COALESCE · NULLIF · IS NOT NULL
Advanced Metrics  │ eFG% · Net Rating · PER · BPM · True Shooting %
                  │ Win Shares · Four Factors · Game Score
Business Patterns │ RFM Segmentation · Outlier Detection · Pivot Table
                  │ Median Calculation · Nth Highest · Duplicate Detection
```

---

## 🔑 Key Insights Found

1. **Home court advantage is real** — home teams win ~57% of games historically, declining slightly in recent seasons
2. **Golden State's dynasty (2014–2018)** was the most dominant run in the dataset — 89-91 wins in three seasons
3. **Russell Westbrook** holds the all-time triple-double game record with 209 triple-doubles in this dataset
4. **3-point shooting has risen ~3% over 20 seasons** — the modern game is dramatically more perimeter-oriented
5. **Luka Dončić** led the 2022-23 season in PPG (31.4) and Game Score (25.60)
6. **eFG% has a stronger correlation with wins than raw FG%** — validating Oliver's Four Factors framework
7. **Teams outperforming their net rating** (lucky teams) tend to regress significantly the following season

---

## 💼 Resume Bullet Points (Copy & Use)

```
• Built a star-schema SQL data warehouse from 900K+ NBA records (5 tables, 20 seasons)
  using SQLite with proper fact/dimension modeling and foreign key relationships

• Wrote 75 validated SQL queries covering aggregation, CTEs, 8 window functions
  (RANK/LAG/LEAD/NTILE/PERCENT_RANK), date analysis, and NULL handling

• Engineered advanced basketball metrics from raw data: eFG%, True Shooting %,
  Player Efficiency Rating, Box Plus/Minus, Win Shares, and Net Rating

• Built interactive Power BI dashboard (5 pages) with 18 DAX measures, bookmarks,
  What-If parameters, sync slicers, and drill-through pages

• Performed outlier detection using z-score methodology in SQL to identify
  statistically anomalous player performances across 668K+ game records
```

---

## 📚 Tools & Technologies

| Tool | Purpose |
|---|---|
| SQLite | Database engine — star schema warehouse |
| DB Browser for SQLite | SQL query execution & exploration |
| Power BI Desktop | 5-page interactive dashboard |
| Excel (xlsxwriter) | Analytics workbook with 5 tabs |
| Python (pandas, sqlite3) | Data pipeline & warehouse build |

---

## 👤 Author

**[Yash Pandey]**  
Data Analyst | SQL · Power BI · Excel · Python  
📧 [pandeyash0303@gmail.com]  
🔗 [https://www.linkedin.com/in/yash-pandey-0728yp/]
