-- ╔══════════════════════════════════════════════════════════════════════════════════╗
-- ║          NBA DATA ANALYTICS — COMPLETE SQL PROJECT                            ║
-- ║          Dataset: 26,651 Games · 668,628 Player Records · 20 Seasons         ║
-- ║          Skills: Aggregation · Window Functions · CTEs · Subqueries          ║
-- ║                  CASE WHEN · JOINs · Date Functions · Performance Metrics    ║
-- ║          Author: [Your Name] | GitHub: [your-github]                         ║
-- ╚══════════════════════════════════════════════════════════════════════════════════╝

-- ──────────────────────────────────────────────────────────────────────────────────
-- DATABASE: NBA_DataWarehouse.db (SQLite)
-- Open with: DB Browser for SQLite · DBeaver · TablePlus · sqlite3 CLI
-- Star Schema Tables:
--   FACT:       fact_games · fact_player_game_stats · fact_standings
--   DIMENSION:  dim_teams · dim_players · dim_date · dim_season
--   ANALYTICS:  analytics_team_season · analytics_player_season
--               analytics_playoff_prob · analytics_h2h · analytics_monthly_trends
-- ──────────────────────────────────────────────────────────────────────────────────


/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  TABLE OF CONTENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  SECTION A  –  SCHEMA SETUP & DATA EXPLORATION           (Q01 – Q05)
  SECTION B  –  TEAM PERFORMANCE ANALYSIS                 (Q06 – Q15)
  SECTION C  –  PLAYER STATISTICS & RANKINGS              (Q16 – Q26)
  SECTION D  –  WINDOW FUNCTIONS & RANKINGS               (Q27 – Q35)
  SECTION E  –  ADVANCED METRICS (eFG%, Net Rating, Pace) (Q36 – Q43)
  SECTION F  –  TIME SERIES & TREND ANALYSIS              (Q44 – Q50)
  SECTION G  –  HEAD-TO-HEAD & MATCHUP ANALYSIS           (Q51 – Q56)
  SECTION H  –  PLAYOFF & STANDINGS ANALYSIS              (Q57 – Q63)
  SECTION I  –  BUSINESS / INTERVIEW QUESTIONS            (Q64 – Q75)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION A  |  SCHEMA SETUP & DATA EXPLORATION  |  Q01–Q05
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q01. [EXPLORATION] What tables exist in the database? How many rows does each have?
-- Skill: Information schema / system tables
-- Why asked: Shows you explore before querying — a must-do in any real project.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT name AS table_name
FROM sqlite_master
WHERE type = 'table'
ORDER BY name;

-- Row counts per table
SELECT 'fact_games'              AS table_name, COUNT(*) AS row_count FROM fact_games UNION ALL
SELECT 'fact_player_game_stats'  , COUNT(*) FROM fact_player_game_stats UNION ALL
SELECT 'fact_standings'          , COUNT(*) FROM fact_standings UNION ALL
SELECT 'dim_teams'               , COUNT(*) FROM dim_teams UNION ALL
SELECT 'dim_players'             , COUNT(*) FROM dim_players UNION ALL
SELECT 'dim_date'                , COUNT(*) FROM dim_date UNION ALL
SELECT 'dim_season'              , COUNT(*) FROM dim_season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q02. [DATA QUALITY] Check for NULL values in the core fact_games table.
-- Skill: NULL handling, data profiling
-- Why asked: Data quality checks are step #1 in any real analytics job.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                    AS total_rows,
    SUM(CASE WHEN pts_home    IS NULL THEN 1 ELSE 0 END) AS null_pts_home,
    SUM(CASE WHEN pts_away    IS NULL THEN 1 ELSE 0 END) AS null_pts_away,
    SUM(CASE WHEN fg_pct_home IS NULL THEN 1 ELSE 0 END) AS null_fg_pct_home,
    SUM(CASE WHEN efg_home    IS NULL THEN 1 ELSE 0 END) AS null_efg_home,
    SUM(CASE WHEN home_team_wins IS NULL THEN 1 ELSE 0 END) AS null_winner,
    ROUND(
        SUM(CASE WHEN pts_home IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                           AS pct_null_pts_home
FROM fact_games;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q03. [EXPLORATION] What is the date range of the dataset? How many seasons and
--      games are covered?
-- Skill: DATE functions, aggregation
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    MIN(game_date_est)                          AS first_game_date,
    MAX(game_date_est)                          AS last_game_date,
    COUNT(DISTINCT season)                      AS total_seasons,
    MIN(season)                                 AS first_season,
    MAX(season)                                 AS last_season,
    COUNT(*)                                    AS total_games,
    COUNT(DISTINCT home_team_id)                AS total_teams
FROM fact_games
WHERE pts_home IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q04. [EXPLORATION] How many games were played per season? Which season had
--      the most games?
-- Skill: GROUP BY, ORDER BY, aggregation
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    g.season,
    s.season_label,
    COUNT(*)                                    AS games_played,
    SUM(CASE WHEN home_team_wins = 1 THEN 1 ELSE 0 END) AS home_wins,
    ROUND(
        SUM(CASE WHEN home_team_wins = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    )                                           AS home_win_pct
FROM fact_games g
JOIN dim_season s ON g.season = s.season
WHERE pts_home IS NOT NULL
GROUP BY g.season, s.season_label
ORDER BY games_played DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q05. [EXPLORATION] Show the full team directory — all 30 franchises with
--      founding year and arena capacity.
-- Skill: Simple SELECT, ORDER BY, CAST
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    abbreviation,
    full_name                                   AS team,
    city,
    arena,
    CAST(arena_capacity AS INTEGER)             AS arena_capacity,
    year_founded,
    (2024 - year_founded)                       AS years_in_league
FROM dim_teams
ORDER BY year_founded ASC;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION B  |  TEAM PERFORMANCE ANALYSIS  |  Q06–Q15
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q06. [AGGREGATION] What are the top 10 teams by all-time wins across all seasons?
-- Skill: Multi-table JOIN, GROUP BY, aggregate, ORDER BY
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    t.full_name                                 AS team,
    t.abbreviation,
    SUM(a.total_wins)                           AS all_time_wins,
    SUM(a.total_losses)                         AS all_time_losses,
    SUM(a.total_games)                          AS all_time_games,
    ROUND(
        SUM(a.total_wins) * 1.0 / SUM(a.total_games), 3
    )                                           AS all_time_win_pct,
    COUNT(DISTINCT a.season)                    AS seasons_played
FROM analytics_team_season a
JOIN dim_teams t ON a.team_id = t.team_id
GROUP BY t.full_name, t.abbreviation
ORDER BY all_time_wins DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q07. [CONDITIONAL AGGREGATION] Classify each team's 2022-23 season performance
--      into tiers: Elite / Contender / Fringe / Rebuilding.
-- Skill: CASE WHEN, aggregation, classification logic
-- Why asked: Data classification is a core analyst task.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    abbreviation,
    total_wins,
    total_losses,
    ROUND(win_pct, 3)                           AS win_pct,
    ROUND(net_rating, 2)                        AS net_rating,
    CASE
        WHEN win_pct >= 0.65                    THEN '🔥 Elite'
        WHEN win_pct >= 0.55                    THEN '💪 Contender'
        WHEN win_pct >= 0.45                    THEN '⚔️  Fringe Playoff'
        WHEN win_pct >= 0.35                    THEN '🔄 Rebuilding'
        ELSE                                         '❌ Lottery'
    END                                         AS performance_tier,
    CASE
        WHEN net_rating > 5                     THEN 'Top Defense & Offense'
        WHEN net_rating > 0                     THEN 'Slightly Positive'
        WHEN net_rating > -5                    THEN 'Below Average'
        ELSE                                         'Poor'
    END                                         AS net_rating_label
FROM analytics_team_season
WHERE season = 2022
ORDER BY win_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q08. [JOIN + AGGREGATION] Which teams have the highest home-court advantage?
--      (Compare home win% vs away win% across all seasons)
-- Skill: Self-join concept, CTEs, derived metrics
-- ─────────────────────────────────────────────────────────────────────────────────
WITH home_record AS (
    SELECT
        home_team_id                            AS team_id,
        COUNT(*)                                AS home_games,
        SUM(home_team_wins)                     AS home_wins
    FROM fact_games
    WHERE pts_home IS NOT NULL
    GROUP BY home_team_id
),
away_record AS (
    SELECT
        visitor_team_id                         AS team_id,
        COUNT(*)                                AS away_games,
        SUM(1 - home_team_wins)                 AS away_wins
    FROM fact_games
    WHERE pts_away IS NOT NULL
    GROUP BY visitor_team_id
)
SELECT
    t.full_name                                 AS team,
    t.abbreviation,
    h.home_games,
    h.home_wins,
    ROUND(h.home_wins * 1.0 / h.home_games, 3) AS home_win_pct,
    a.away_games,
    a.away_wins,
    ROUND(a.away_wins * 1.0 / a.away_games, 3) AS away_win_pct,
    ROUND(
        (h.home_wins * 1.0 / h.home_games) -
        (a.away_wins * 1.0 / a.away_games), 3
    )                                           AS home_advantage_gap
FROM home_record h
JOIN away_record a   ON h.team_id = a.team_id
JOIN dim_teams t     ON h.team_id = t.team_id
ORDER BY home_advantage_gap DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q09. [AGGREGATION] What is the average, max, and min points scored per game
--      for each team in the 2022-23 season?
-- Skill: Multiple aggregate functions, GROUP BY
-- ─────────────────────────────────────────────────────────────────────────────────
WITH all_pts AS (
    SELECT home_team_id AS team_id, pts_home AS pts, season
    FROM fact_games WHERE pts_home IS NOT NULL
    UNION ALL
    SELECT visitor_team_id, pts_away, season
    FROM fact_games WHERE pts_away IS NOT NULL
)
SELECT
    t.full_name                                 AS team,
    t.abbreviation,
    COUNT(*)                                    AS games_played,
    ROUND(AVG(a.pts), 1)                        AS avg_pts,
    MAX(a.pts)                                  AS max_pts,
    MIN(a.pts)                                  AS min_pts,
    ROUND(MAX(a.pts) - MIN(a.pts), 0)           AS pts_range,
    -- Standard-deviation proxy (manual calculation)
    ROUND(
        SQRT(AVG(a.pts * a.pts) - AVG(a.pts) * AVG(a.pts)), 2
    )                                           AS pts_std_dev
FROM all_pts a
JOIN dim_teams t ON a.team_id = t.team_id
WHERE a.season = 2022
GROUP BY t.full_name, t.abbreviation
ORDER BY avg_pts DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q10. [CTE + AGGREGATION] Which teams had 55+ wins in a season more than once?
--      (Sustained excellence indicator)
-- Skill: CTEs, HAVING, COUNT of conditions
-- ─────────────────────────────────────────────────────────────────────────────────
WITH elite_seasons AS (
    SELECT
        team_id,
        full_name,
        season,
        season_label,
        total_wins,
        win_pct
    FROM analytics_team_season
    WHERE total_wins >= 55
)
SELECT
    full_name                                   AS team,
    COUNT(*)                                    AS elite_seasons_count,
    MIN(season)                                 AS first_elite_season,
    MAX(season)                                 AS last_elite_season,
    MAX(total_wins)                             AS best_win_total,
    ROUND(AVG(win_pct), 3)                      AS avg_win_pct_in_elite_seasons
FROM elite_seasons
GROUP BY team_id, full_name
HAVING elite_seasons_count >= 2
ORDER BY elite_seasons_count DESC, best_win_total DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q11. [SUBQUERY] Find all teams that scored above the league average points
--      per game in the 2022-23 season.
-- Skill: Subquery in WHERE clause, comparison against aggregate
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    abbreviation,
    ROUND(avg_pts_scored, 1)                    AS team_avg_pts,
    ROUND(
        (SELECT AVG(avg_pts_scored) FROM analytics_team_season WHERE season = 2022), 1
    )                                           AS league_avg_pts,
    ROUND(
        avg_pts_scored -
        (SELECT AVG(avg_pts_scored) FROM analytics_team_season WHERE season = 2022), 1
    )                                           AS vs_league_avg
FROM analytics_team_season
WHERE season = 2022
  AND avg_pts_scored > (
        SELECT AVG(avg_pts_scored)
        FROM analytics_team_season
        WHERE season = 2022
      )
ORDER BY avg_pts_scored DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q12. [CASE WHEN + GROUP BY] Segment all 2022-23 teams by their shooting
--      efficiency profile (eFG% bracket).
-- Skill: CASE WHEN segmentation, GROUP BY with count
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN avg_efg_pct >= 0.560               THEN 'A — Elite Shooter (≥56%)'
        WHEN avg_efg_pct >= 0.545               THEN 'B — Good Shooter (54.5–56%)'
        WHEN avg_efg_pct >= 0.530               THEN 'C — Average (53–54.5%)'
        ELSE                                         'D — Below Average (<53%)'
    END                                         AS efg_tier,
    COUNT(*)                                    AS teams_in_tier,
    ROUND(AVG(win_pct), 3)                      AS avg_win_pct,
    ROUND(AVG(net_rating), 2)                   AS avg_net_rating,
    GROUP_CONCAT(abbreviation, ' · ')           AS teams
FROM analytics_team_season
WHERE season = 2022
GROUP BY efg_tier
ORDER BY avg_win_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q13. [MULTI-JOIN] Show each team's 2022-23 performance with their arena info.
-- Skill: Multi-table JOIN, column selection from multiple tables
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    t.full_name                                 AS team,
    t.city,
    t.arena,
    CAST(t.arena_capacity AS INTEGER)           AS capacity,
    a.total_wins,
    a.total_losses,
    ROUND(a.win_pct, 3)                         AS win_pct,
    ROUND(a.net_rating, 2)                      AS net_rating,
    ROUND(a.avg_efg_pct * 100, 1)               AS efg_pct,
    ROUND(a.pace_estimate, 1)                   AS pace,
    t.year_founded
FROM analytics_team_season a
JOIN dim_teams t ON a.team_id = t.team_id
WHERE a.season = 2022
ORDER BY a.win_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q14. [AGGREGATION] What were the highest-scoring individual games ever recorded?
--      Show top 20 combined-score games.
-- Skill: Expression columns, multi-table JOIN, ORDER BY
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    g.game_date_est,
    g.season,
    ht.full_name                                AS home_team,
    at_.full_name                               AS away_team,
    g.pts_home,
    g.pts_away,
    (g.pts_home + g.pts_away)                   AS combined_score,
    ABS(g.pts_home - g.pts_away)                AS margin,
    CASE
        WHEN g.home_team_wins = 1               THEN ht.full_name
        ELSE at_.full_name
    END                                         AS winner
FROM fact_games g
JOIN dim_teams ht  ON g.home_team_id      = ht.team_id
JOIN dim_teams at_ ON g.visitor_team_id   = at_.team_id
WHERE g.pts_home IS NOT NULL
ORDER BY combined_score DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q15. [FILTERING + AGGREGATION] Find all blowout wins (margin ≥ 30 points).
--      Which team has the most blowout wins? Which suffered the most blowout losses?
-- Skill: CASE WHEN in aggregation, multi-column grouping
-- ─────────────────────────────────────────────────────────────────────────────────
WITH blowouts AS (
    SELECT
        g.season,
        ht.full_name                            AS home_team,
        at_.full_name                           AS away_team,
        g.pts_home,
        g.pts_away,
        ABS(g.pts_home - g.pts_away)            AS margin,
        CASE
            WHEN g.home_team_wins = 1           THEN ht.full_name
            ELSE at_.full_name
        END                                     AS winner,
        CASE
            WHEN g.home_team_wins = 0           THEN ht.full_name
            ELSE at_.full_name
        END                                     AS loser
    FROM fact_games g
    JOIN dim_teams ht  ON g.home_team_id     = ht.team_id
    JOIN dim_teams at_ ON g.visitor_team_id  = at_.team_id
    WHERE ABS(g.pts_home - g.pts_away) >= 30
      AND g.pts_home IS NOT NULL
)
SELECT
    t.full_name                                 AS team,
    SUM(CASE WHEN b.winner = t.full_name THEN 1 ELSE 0 END) AS blowout_wins,
    SUM(CASE WHEN b.loser  = t.full_name THEN 1 ELSE 0 END) AS blowout_losses,
    SUM(CASE WHEN b.winner = t.full_name THEN 1 ELSE 0 END) -
    SUM(CASE WHEN b.loser  = t.full_name THEN 1 ELSE 0 END) AS net_blowout_record
FROM blowouts b
CROSS JOIN dim_teams t
WHERE t.full_name IN (b.winner, b.loser)
GROUP BY t.full_name
ORDER BY blowout_wins DESC
LIMIT 15;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION C  |  PLAYER STATISTICS & RANKINGS  |  Q16–Q26
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q16. [AGGREGATION] Who are the top 20 scorers in the 2022-23 season?
--      Include PPG, RPG, APG, FG%, and 3P%.
-- Skill: Multi-metric aggregation, HAVING, MIN game filter
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    player_name,
    team_abbreviation                           AS team,
    games_played,
    ROUND(avg_pts, 1)                           AS ppg,
    ROUND(avg_reb, 1)                           AS rpg,
    ROUND(avg_ast, 1)                           AS apg,
    ROUND(avg_stl, 1)                           AS spg,
    ROUND(avg_blk, 1)                           AS bpg,
    ROUND(avg_fg_pct * 100, 1)                  AS fg_pct,
    ROUND(avg_fg3_pct * 100, 1)                 AS fg3_pct,
    ROUND(avg_plus_minus, 1)                    AS plus_minus,
    ROUND(avg_game_score, 2)                    AS game_score
FROM analytics_player_season
WHERE season = 2022
  AND games_played >= 15
ORDER BY avg_pts DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q17. [AGGREGATION] Who are the all-time leaders in total points scored
--      across all seasons in this dataset?
-- Skill: GROUP BY across full dataset, SUM, ranking
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    player_name,
    COUNT(DISTINCT season)                      AS seasons_in_dataset,
    SUM(games_played)                           AS career_games,
    SUM(total_pts)                              AS career_points,
    ROUND(SUM(total_pts) * 1.0 / SUM(games_played), 1) AS career_ppg,
    ROUND(MAX(avg_pts), 1)                      AS best_season_ppg,
    MAX(season)                                 AS last_season
FROM analytics_player_season
WHERE games_played >= 10
GROUP BY player_name
ORDER BY career_points DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q18. [CTE + SUBQUERY] Find players who averaged a triple-double (10+ in all
--      three major stats: PTS, REB, AST) in any season.
-- Skill: CTEs, multiple conditions, CASE labeling
-- ─────────────────────────────────────────────────────────────────────────────────
WITH triple_double_seasons AS (
    SELECT
        player_name,
        team_abbreviation                       AS team,
        season,
        ROUND(avg_pts, 1)                       AS ppg,
        ROUND(avg_reb, 1)                       AS rpg,
        ROUND(avg_ast, 1)                       AS apg,
        games_played,
        ROUND(avg_game_score, 2)                AS game_score
    FROM analytics_player_season
    WHERE avg_pts  >= 10
      AND avg_reb  >= 10
      AND avg_ast  >= 10
      AND games_played >= 20
)
SELECT
    player_name,
    team,
    season,
    ppg,
    rpg,
    apg,
    games_played,
    game_score,
    ROUND(ppg + rpg + apg, 1)                  AS combined_stats
FROM triple_double_seasons
ORDER BY combined_stats DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q19. [AGGREGATION + CASE] Count how many times each player recorded a
--      "game triple-double" (10+ PTS, 10+ REB, 10+ AST in one game) — all time.
-- Skill: CASE WHEN inside SUM, individual game filtering
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS last_team,
    COUNT(*)                                    AS triple_double_games,
    SUM(CASE WHEN p.pts  >= 20 AND p.reb >= 10 AND p.ast >= 10 THEN 1 ELSE 0 END) AS double_triple_doubles,
    ROUND(AVG(p.pts), 1)                        AS avg_pts_in_td_games,
    ROUND(AVG(p.reb), 1)                        AS avg_reb_in_td_games,
    ROUND(AVG(p.ast), 1)                        AS avg_ast_in_td_games
FROM fact_player_game_stats p
WHERE p.pts >= 10
  AND p.reb >= 10
  AND p.ast >= 10
  AND p.minutes_played > 10
GROUP BY p.player_name, p.team_abbreviation
ORDER BY triple_double_games DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q20. [AGGREGATION] Who had the most 40+ point individual game performances?
-- Skill: HAVING vs WHERE, counting filtered rows
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    COUNT(*)                                    AS games_40plus,
    MAX(p.pts)                                  AS single_game_max,
    ROUND(AVG(p.pts), 1)                        AS avg_pts_in_40plus_games,
    SUM(CASE WHEN p.pts >= 50 THEN 1 ELSE 0 END) AS games_50plus,
    SUM(CASE WHEN p.pts >= 60 THEN 1 ELSE 0 END) AS games_60plus
FROM fact_player_game_stats p
WHERE p.pts >= 40
  AND p.minutes_played >= 20
GROUP BY p.player_name, p.team_abbreviation
ORDER BY games_40plus DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q21. [CTE + JOIN] Identify "double-double" players per season —
--      averaged 10+ points AND 10+ rebounds.
-- Skill: CTE filtering, multi-condition WHERE
-- ─────────────────────────────────────────────────────────────────────────────────
WITH double_doubles AS (
    SELECT
        player_name,
        team_abbreviation,
        season,
        ROUND(avg_pts, 1)                       AS ppg,
        ROUND(avg_reb, 1)                       AS rpg,
        ROUND(avg_ast, 1)                       AS apg,
        games_played,
        ROUND(avg_game_score, 2)                AS efficiency
    FROM analytics_player_season
    WHERE avg_pts >= 10
      AND avg_reb >= 10
      AND games_played >= 20
)
SELECT *
FROM double_doubles
ORDER BY season DESC, ppg DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q22. [SUBQUERY + AGGREGATION] Find players who were top-5 scorers in their
--      team's lineup in the 2022-23 season.
-- Skill: Subquery with IN, filtering within groups
-- ─────────────────────────────────────────────────────────────────────────────────
WITH team_scorer_rank AS (
    SELECT
        player_name,
        team_abbreviation,
        season,
        avg_pts,
        avg_game_score,
        games_played,
        ROW_NUMBER() OVER (
            PARTITION BY team_abbreviation, season
            ORDER BY avg_pts DESC
        )                                       AS scorer_rank_on_team
    FROM analytics_player_season
    WHERE games_played >= 10
)
SELECT
    player_name,
    team_abbreviation                           AS team,
    scorer_rank_on_team                         AS team_rank,
    ROUND(avg_pts, 1)                           AS ppg,
    ROUND(avg_game_score, 2)                    AS game_score,
    games_played
FROM team_scorer_rank
WHERE season = 2022
  AND scorer_rank_on_team <= 2
ORDER BY team_abbreviation, scorer_rank_on_team;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q23. [AGGREGATION] What is the average Game Score by starting position (G/F/C)?
--      Which position is most efficient in terms of scoring, rebounding, assists?
-- Skill: GROUP BY non-numeric, multi-metric aggregation
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN start_position = 'G'               THEN 'Guard'
        WHEN start_position = 'F'               THEN 'Forward'
        WHEN start_position = 'C'               THEN 'Center'
        ELSE 'Other / Bench'
    END                                         AS position,
    COUNT(*)                                    AS player_game_records,
    ROUND(AVG(pts), 1)                          AS avg_pts,
    ROUND(AVG(reb), 1)                          AS avg_reb,
    ROUND(AVG(ast), 1)                          AS avg_ast,
    ROUND(AVG(stl), 2)                          AS avg_stl,
    ROUND(AVG(blk), 2)                          AS avg_blk,
    ROUND(AVG(fg_pct), 3)                       AS avg_fg_pct,
    ROUND(AVG(game_score), 2)                   AS avg_game_score,
    ROUND(AVG(plus_minus), 2)                   AS avg_plus_minus
FROM fact_player_game_stats
WHERE minutes_played >= 15
  AND start_position IN ('G', 'F', 'C')
GROUP BY start_position
ORDER BY avg_game_score DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q24. [AGGREGATION + FILTERING] Which players had the best plus/minus average
--      in the 2022-23 season? (Min. 20 games, 20 minutes/game)
-- Skill: Multiple filters, derived metric ranking
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    COUNT(DISTINCT p.game_id)                   AS games,
    ROUND(AVG(p.plus_minus), 2)                 AS avg_plus_minus,
    ROUND(AVG(p.pts), 1)                        AS avg_pts,
    ROUND(AVG(p.minutes_played), 1)             AS avg_minutes,
    ROUND(AVG(p.game_score), 2)                 AS avg_game_score
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
  AND p.minutes_played >= 20
GROUP BY p.player_name, p.team_abbreviation
HAVING games >= 20
ORDER BY avg_plus_minus DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q25. [ADVANCED AGGREGATION] Calculate True Shooting % (TS%) for top players.
--      TS% = PTS / (2 * (FGA + 0.44 * FTA))
-- Skill: Formula-derived metrics, complex expressions
-- Why asked: Shows domain knowledge of advanced basketball statistics.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    COUNT(DISTINCT p.game_id)                   AS games,
    ROUND(AVG(p.pts), 1)                        AS avg_pts,
    ROUND(AVG(p.fga), 1)                        AS avg_fga,
    ROUND(AVG(p.fta), 1)                        AS avg_fta,
    ROUND(AVG(p.fg_pct) * 100, 1)              AS fg_pct,
    ROUND(
        SUM(p.pts) * 1.0 /
        NULLIF(2.0 * (SUM(p.fga) + 0.44 * SUM(p.fta)), 0) * 100, 1
    )                                           AS true_shooting_pct,
    ROUND(AVG(p.game_score), 2)                 AS avg_game_score
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
  AND p.minutes_played >= 15
GROUP BY p.player_name, p.team_abbreviation
HAVING games >= 20
   AND SUM(p.fga) > 0
ORDER BY true_shooting_pct DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q26. [CTE + AGGREGATION] Find players whose scoring improved the most
--      from 2019 to 2022 season (season-over-season growth).
-- Skill: Self-join on the same table, computed change, CTEs
-- ─────────────────────────────────────────────────────────────────────────────────
WITH s2019 AS (
    SELECT player_name, team_abbreviation, avg_pts AS ppg_2019
    FROM analytics_player_season
    WHERE season = 2019 AND games_played >= 20
),
s2022 AS (
    SELECT player_name, team_abbreviation, avg_pts AS ppg_2022
    FROM analytics_player_season
    WHERE season = 2022 AND games_played >= 20
)
SELECT
    s2022.player_name,
    s2019.team_abbreviation                     AS team_2019,
    s2022.team_abbreviation                     AS team_2022,
    ROUND(s2019.ppg_2019, 1)                    AS ppg_2019,
    ROUND(s2022.ppg_2022, 1)                    AS ppg_2022,
    ROUND(s2022.ppg_2022 - s2019.ppg_2019, 1)   AS ppg_increase,
    ROUND(
        (s2022.ppg_2022 - s2019.ppg_2019) * 100.0 / s2019.ppg_2019, 1
    )                                           AS pct_growth
FROM s2022
JOIN s2019 ON s2022.player_name = s2019.player_name
WHERE s2022.ppg_2022 > s2019.ppg_2019
ORDER BY ppg_increase DESC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION D  |  WINDOW FUNCTIONS & RANKINGS  |  Q27–Q35
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q27. [WINDOW — RANK] Rank all teams by win% within each season using
--      DENSE_RANK and ROW_NUMBER. Show both — highlight the difference.
-- Skill: ROW_NUMBER vs RANK vs DENSE_RANK — a classic interview question.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    season,
    full_name                                   AS team,
    abbreviation,
    total_wins,
    total_losses,
    ROUND(win_pct, 3)                           AS win_pct,
    ROW_NUMBER()  OVER (PARTITION BY season ORDER BY win_pct DESC) AS row_number_rank,
    RANK()        OVER (PARTITION BY season ORDER BY win_pct DESC) AS rank,
    DENSE_RANK()  OVER (PARTITION BY season ORDER BY win_pct DESC) AS dense_rank
FROM analytics_team_season
WHERE season IN (2018, 2019, 2020, 2021, 2022)
ORDER BY season DESC, win_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q28. [WINDOW — LAG/LEAD] Show each team's win total and compare it to the
--      previous season and next season (year-over-year change).
-- Skill: LAG() and LEAD() window functions — very common in analytics interviews.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    abbreviation,
    season,
    total_wins,
    LAG(total_wins, 1)  OVER (PARTITION BY team_id ORDER BY season) AS prev_season_wins,
    LEAD(total_wins, 1) OVER (PARTITION BY team_id ORDER BY season) AS next_season_wins,
    total_wins -
        LAG(total_wins, 1) OVER (PARTITION BY team_id ORDER BY season) AS yoy_change,
    CASE
        WHEN total_wins -
             LAG(total_wins, 1) OVER (PARTITION BY team_id ORDER BY season) > 5
        THEN '📈 Improved'
        WHEN total_wins -
             LAG(total_wins, 1) OVER (PARTITION BY team_id ORDER BY season) < -5
        THEN '📉 Declined'
        ELSE '➡️  Stable'
    END                                         AS trend
FROM analytics_team_season
ORDER BY full_name, season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q29. [WINDOW — RUNNING TOTAL] Calculate the cumulative wins for the
--      Boston Celtics across all seasons (running total).
-- Skill: SUM() OVER with ORDER BY — running total pattern
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    a.season,
    s.season_label,
    a.total_wins,
    a.total_losses,
    ROUND(a.win_pct, 3)                         AS win_pct,
    SUM(a.total_wins)    OVER (ORDER BY a.season) AS cumulative_wins,
    SUM(a.total_losses)  OVER (ORDER BY a.season) AS cumulative_losses,
    SUM(a.total_games)   OVER (ORDER BY a.season) AS cumulative_games,
    ROUND(
        SUM(a.total_wins) OVER (ORDER BY a.season) * 1.0 /
        SUM(a.total_games) OVER (ORDER BY a.season), 3
    )                                           AS cumulative_win_pct
FROM analytics_team_season a
JOIN dim_teams t   ON a.team_id = t.team_id
JOIN dim_season s  ON a.season  = s.season
WHERE t.abbreviation = 'BOS'
ORDER BY a.season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q30. [WINDOW — MOVING AVERAGE] 3-season rolling average win% for each team.
-- Skill: AVG() OVER with ROWS BETWEEN — rolling/moving average pattern.
-- Why asked: Moving averages are a core data analyst skill.
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    abbreviation,
    season,
    total_wins,
    ROUND(win_pct, 3)                           AS win_pct,
    ROUND(
        AVG(win_pct) OVER (
            PARTITION BY team_id
            ORDER BY season
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 3
    )                                           AS rolling_3yr_win_pct,
    ROUND(
        AVG(net_rating) OVER (
            PARTITION BY team_id
            ORDER BY season
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    )                                           AS rolling_3yr_net_rating
FROM analytics_team_season
ORDER BY full_name, season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q31. [WINDOW — NTILE] Divide all 2022-23 players into performance quartiles
--      based on average points scored (NTILE 4-bucket segmentation).
-- Skill: NTILE() — percentile/bucket segmentation
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    player_name,
    team_abbreviation                           AS team,
    ROUND(avg_pts, 1)                           AS ppg,
    ROUND(avg_game_score, 2)                    AS game_score,
    games_played,
    NTILE(4) OVER (ORDER BY avg_pts DESC)       AS scoring_quartile,
    CASE NTILE(4) OVER (ORDER BY avg_pts DESC)
        WHEN 1 THEN '🥇 Top 25% Scorers'
        WHEN 2 THEN '🥈 Upper-Mid Scorers'
        WHEN 3 THEN '🥉 Lower-Mid Scorers'
        WHEN 4 THEN '⬇️  Bottom 25% Scorers'
    END                                         AS scoring_tier
FROM analytics_player_season
WHERE season = 2022
  AND games_played >= 20
ORDER BY scoring_quartile, avg_pts DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q32. [WINDOW — FIRST_VALUE / LAST_VALUE] For each team, show the best and
--      worst season win totals alongside every season record.
-- Skill: FIRST_VALUE / LAST_VALUE / MIN MAX over partition
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    abbreviation,
    season,
    total_wins,
    ROUND(win_pct, 3)                           AS win_pct,
    MAX(total_wins) OVER (PARTITION BY team_id) AS franchise_best_wins,
    MIN(total_wins) OVER (PARTITION BY team_id) AS franchise_worst_wins,
    FIRST_VALUE(season) OVER (
        PARTITION BY team_id
        ORDER BY total_wins DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                           AS best_season_year,
    ROUND(AVG(win_pct) OVER (PARTITION BY team_id), 3) AS franchise_avg_win_pct
FROM analytics_team_season
ORDER BY full_name, season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q33. [WINDOW — PERCENT_RANK] Show each team's win% percentile ranking
--      across ALL seasons in one unified view.
-- Skill: PERCENT_RANK() and CUME_DIST() — percentile analysis
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    season,
    total_wins,
    ROUND(win_pct, 3)                           AS win_pct,
    ROUND(PERCENT_RANK() OVER (
        PARTITION BY season ORDER BY win_pct
    ) * 100, 1)                                 AS percentile_in_season,
    ROUND(CUME_DIST() OVER (
        PARTITION BY season ORDER BY win_pct
    ) * 100, 1)                                 AS cumulative_dist_pct
FROM analytics_team_season
WHERE season = 2022
ORDER BY win_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q34. [WINDOW — PARTITION BY MULTI-COL] Rank players by PPG within their
--      own team for each season — find each team's #1 scorer per year.
-- Skill: RANK() with PARTITION BY two columns
-- ─────────────────────────────────────────────────────────────────────────────────
WITH team_scoring_leaders AS (
    SELECT
        player_name,
        team_abbreviation                       AS team,
        season,
        ROUND(avg_pts, 1)                       AS ppg,
        ROUND(avg_reb, 1)                       AS rpg,
        ROUND(avg_ast, 1)                       AS apg,
        games_played,
        RANK() OVER (
            PARTITION BY team_abbreviation, season
            ORDER BY avg_pts DESC
        )                                       AS team_scoring_rank
    FROM analytics_player_season
    WHERE games_played >= 15
)
SELECT *
FROM team_scoring_leaders
WHERE team_scoring_rank = 1
  AND season >= 2019
ORDER BY season DESC, ppg DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q35. [WINDOW — ADVANCED] Find the most consistent scorers: lowest standard
--      deviation in points scored across games (min 30 games in a season).
-- Skill: Manual STDEV using window AVG, nested calculations
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    g.season,
    COUNT(DISTINCT p.game_id)                   AS games,
    ROUND(AVG(p.pts), 1)                        AS avg_pts,
    ROUND(
        SQRT(AVG(p.pts * p.pts) - AVG(p.pts) * AVG(p.pts)), 2
    )                                           AS scoring_std_dev,
    ROUND(
        SQRT(AVG(p.pts * p.pts) - AVG(p.pts) * AVG(p.pts)) /
        NULLIF(AVG(p.pts), 0) * 100, 1
    )                                           AS coeff_of_variation_pct,
    MIN(p.pts)                                  AS min_pts,
    MAX(p.pts)                                  AS max_pts
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
  AND p.minutes_played >= 20
GROUP BY p.player_name, p.team_abbreviation, g.season
HAVING games >= 30
   AND avg_pts >= 15
ORDER BY scoring_std_dev ASC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION E  |  ADVANCED METRICS — eFG%, NET RATING, PACE  |  Q36–Q43
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q36. [METRICS] Calculate Effective Field Goal % (eFG%) from raw game data.
--      eFG% = (FGM + 0.5 * FG3M) / FGA — the #1 Four Factor of NBA efficiency.
-- Skill: Custom metric formula, aggregation from raw data
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.team_abbreviation                         AS team,
    g.season,
    SUM(p.fgm)                                  AS total_fgm,
    SUM(p.fg3m)                                 AS total_fg3m,
    SUM(p.fga)                                  AS total_fga,
    ROUND(
        (SUM(p.fgm) + 0.5 * SUM(p.fg3m)) * 100.0 /
        NULLIF(SUM(p.fga), 0), 2
    )                                           AS efg_pct,
    ROUND(SUM(p.fg3m) * 100.0 / NULLIF(SUM(p.fg3a), 0), 2) AS three_pt_pct,
    ROUND(SUM(p.ftm) * 100.0 / NULLIF(SUM(p.fta), 0), 2)   AS ft_pct
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
GROUP BY p.team_abbreviation, g.season
ORDER BY efg_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q37. [METRICS] Build the full Four Factors comparison for 2022-23.
--      eFG% · TOV% · OREB% · FT Rate — Oliver's framework for winning basketball.
-- Skill: Multiple formulas, multi-metric analysis
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    t.full_name                                 AS team,
    t.abbreviation,
    -- Four Factors (Offense)
    ROUND(a.avg_efg_pct * 100, 2)               AS efg_pct,
    ROUND(a.avg_fg_pct  * 100, 2)               AS fg_pct,
    ROUND(a.avg_fg3_pct * 100, 2)               AS fg3_pct,
    ROUND(a.avg_ft_pct  * 100, 2)               AS ft_pct,
    ROUND(a.avg_ast, 1)                         AS avg_ast,
    ROUND(a.avg_reb, 1)                         AS avg_reb,
    -- Overall
    ROUND(a.net_rating, 2)                      AS net_rating,
    ROUND(a.pace_estimate, 1)                   AS pace,
    ROUND(a.win_pct, 3)                         AS win_pct,
    a.total_wins                                AS wins,
    -- Composite Efficiency Score (custom metric)
    ROUND(
        (a.avg_efg_pct * 40) +
        (a.avg_ft_pct  * 15) +
        (a.avg_ast / 30 * 25) +
        (a.avg_reb / 50 * 20), 2
    )                                           AS composite_efficiency_score
FROM analytics_team_season a
JOIN dim_teams t ON a.team_id = t.team_id
WHERE a.season = 2022
ORDER BY composite_efficiency_score DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q38. [NET RATING] Which teams had the best Net Rating each season?
--      Net Rating = Points Scored per 100 possessions - Points Allowed per 100.
-- Skill: MAX with PARTITION, year-by-year best performer
-- ─────────────────────────────────────────────────────────────────────────────────
WITH season_net_ranks AS (
    SELECT
        season,
        full_name                               AS team,
        abbreviation,
        ROUND(net_rating, 2)                    AS net_rating,
        total_wins,
        ROUND(win_pct, 3)                       AS win_pct,
        RANK() OVER (PARTITION BY season ORDER BY net_rating DESC) AS season_rank
    FROM analytics_team_season
)
SELECT
    season,
    team,
    abbreviation,
    net_rating,
    total_wins,
    win_pct,
    season_rank
FROM season_net_ranks
WHERE season_rank <= 3
ORDER BY season DESC, season_rank;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q39. [PACE ANALYSIS] Correlate team pace with points scored.
--      Do faster teams score more? Build a correlation proxy.
-- Skill: Multiple metrics, sorting by correlation
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    season,
    ROUND(AVG(pace_estimate), 1)                AS avg_league_pace,
    ROUND(MIN(pace_estimate), 1)                AS slowest_pace,
    ROUND(MAX(pace_estimate), 1)                AS fastest_pace,
    ROUND(AVG(avg_pts_scored), 1)               AS avg_league_pts,
    ROUND(MIN(avg_pts_scored), 1)               AS lowest_scoring_team,
    ROUND(MAX(avg_pts_scored), 1)               AS highest_scoring_team,
    -- Pace bracket
    COUNT(CASE WHEN pace_estimate >= 100 THEN 1 END) AS fast_paced_teams,
    COUNT(CASE WHEN pace_estimate <  100 THEN 1 END) AS slow_paced_teams
FROM analytics_team_season
GROUP BY season
ORDER BY season DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q40. [ADVANCED] Calculate Player Efficiency Rating (PER) approximation.
--      Simplified PER = (PTS + REB + AST + STL + BLK - TO - missed FG) / MIN
-- Skill: Multi-field formula, per-minute normalization
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    g.season,
    COUNT(DISTINCT p.game_id)                   AS games,
    ROUND(AVG(p.pts), 1)                        AS ppg,
    ROUND(AVG(p.minutes_played), 1)             AS mpg,
    ROUND(
        SUM(p.pts + p.reb + p.ast + p.stl + p.blk
            - COALESCE(p."to", 0)
            - (COALESCE(p.fga, 0) - COALESCE(p.fgm, 0))
            - (COALESCE(p.fta, 0) - COALESCE(p.ftm, 0)) * 0.5
        ) / NULLIF(SUM(p.minutes_played), 0) * 36, 2
    )                                           AS per_36_approx,
    ROUND(AVG(p.game_score), 2)                 AS avg_game_score
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
  AND p.minutes_played >= 15
GROUP BY p.player_name, p.team_abbreviation, g.season
HAVING games >= 25
ORDER BY per_36_approx DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q41. [ADVANCED] Box Plus/Minus (BPM) approximation per player in 2022-23.
--      BPM ≈ (PTS + 0.3*AST + 0.4*REB + 0.7*BLK + 0.7*STL - 0.6*TO) / games
-- Skill: Weighted composite metric, normalization
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    COUNT(DISTINCT p.game_id)                   AS games,
    ROUND(
        (SUM(p.pts)
         + 0.3 * SUM(p.ast)
         + 0.4 * SUM(p.reb)
         + 0.7 * SUM(p.blk)
         + 0.7 * SUM(p.stl)
         - 0.6 * SUM(COALESCE(p."to", 0))
        ) / NULLIF(COUNT(DISTINCT p.game_id), 0), 2
    )                                           AS bpm_approx,
    ROUND(AVG(p.plus_minus), 2)                 AS avg_plus_minus,
    ROUND(AVG(p.pts), 1)                        AS ppg
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
  AND p.minutes_played >= 20
GROUP BY p.player_name, p.team_abbreviation
HAVING games >= 25
ORDER BY bpm_approx DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q42. [AGGREGATION] What percentage of each team's offense came from 3-pointers
--      vs 2-pointers vs free throws in 2022-23?
-- Skill: CASE-based decomposition, percentage breakdown
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.team_abbreviation                         AS team,
    ROUND(SUM(p.pts), 0)                        AS total_pts,
    ROUND(SUM(p.fg3m) * 3, 0)                   AS pts_from_3pt,
    ROUND(
        (SUM(p.fgm) - SUM(p.fg3m)) * 2, 0
    )                                           AS pts_from_2pt,
    ROUND(SUM(p.ftm), 0)                        AS pts_from_ft,
    -- Percentages
    ROUND(SUM(p.fg3m) * 3 * 100.0 / NULLIF(SUM(p.pts), 0), 1) AS pct_from_3pt,
    ROUND(
        (SUM(p.fgm) - SUM(p.fg3m)) * 2 * 100.0 / NULLIF(SUM(p.pts), 0), 1
    )                                           AS pct_from_2pt,
    ROUND(SUM(p.ftm) * 100.0 / NULLIF(SUM(p.pts), 0), 1) AS pct_from_ft
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
WHERE g.season = 2022
GROUP BY p.team_abbreviation
ORDER BY pct_from_3pt DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q43. [ADVANCED METRIC] Calculate Win Shares approximation per team-season.
--      Win Shares ≈ (Net Rating / 28) * Games * 0.57  [simplified model]
-- Skill: Formula derivation, metric explanation, ranking
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    full_name                                   AS team,
    abbreviation,
    season,
    total_wins,
    ROUND(net_rating, 2)                        AS net_rating,
    ROUND(net_rating / 28.0 * total_games * 0.57, 1) AS win_shares_approx,
    ROUND(total_wins - (net_rating / 28.0 * total_games * 0.57), 1) AS luck_factor,
    CASE
        WHEN total_wins - (net_rating / 28.0 * total_games * 0.57) > 3  THEN '🍀 Lucky (Outperformed)'
        WHEN total_wins - (net_rating / 28.0 * total_games * 0.57) < -3 THEN '📉 Unlucky (Underperformed)'
        ELSE '⚖️  As Expected'
    END                                         AS performance_vs_expected
FROM analytics_team_season
WHERE season = 2022
ORDER BY win_shares_approx DESC;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION F  |  TIME SERIES & TREND ANALYSIS  |  Q44–Q50
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q44. [TIME SERIES] How has the NBA's average points per game changed
--      year over year? Is the league trending toward more scoring?
-- Skill: GROUP BY season, aggregate, year-over-year change, LAG
-- ─────────────────────────────────────────────────────────────────────────────────
WITH yearly_scoring AS (
    SELECT
        season,
        ROUND(AVG(pts_home + pts_away), 1)      AS avg_combined_pts,
        ROUND(AVG(pts_home), 1)                 AS avg_home_pts,
        ROUND(AVG(pts_away), 1)                 AS avg_away_pts,
        ROUND(AVG(fg3_pct_home + fg3_pct_away) / 2 * 100, 1) AS avg_3pt_pct,
        COUNT(*)                                AS games
    FROM fact_games
    WHERE pts_home IS NOT NULL
    GROUP BY season
)
SELECT
    season,
    avg_combined_pts,
    avg_home_pts,
    avg_away_pts,
    avg_3pt_pct,
    games,
    LAG(avg_combined_pts) OVER (ORDER BY season) AS prev_yr_avg,
    ROUND(
        avg_combined_pts - LAG(avg_combined_pts) OVER (ORDER BY season), 1
    )                                           AS yoy_pts_change
FROM yearly_scoring
ORDER BY season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q45. [DATE FUNCTIONS] Which months have the highest scoring games?
--      (Do December games score differently than March/April games?)
-- Skill: strftime date extraction, GROUP BY date part
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    strftime('%m', game_date_est)               AS month_num,
    CASE strftime('%m', game_date_est)
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May (Playoffs)'
        WHEN '06' THEN 'June (Finals)'
        ELSE            'Other'
    END                                         AS month_name,
    COUNT(*)                                    AS games_played,
    ROUND(AVG(pts_home + pts_away), 1)          AS avg_combined_pts,
    ROUND(AVG(pts_home), 1)                     AS avg_home_pts,
    ROUND(AVG(pts_away), 1)                     AS avg_away_pts,
    ROUND(AVG(ABS(pts_home - pts_away)), 1)     AS avg_margin
FROM fact_games
WHERE pts_home IS NOT NULL
GROUP BY month_num
ORDER BY month_num;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q46. [TIME TREND] Track the Golden State Warriors' performance across all seasons.
--      Show wins, net rating, eFG%, and the dynasty period.
-- Skill: Filter by team, time-series analysis, trend markers
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    a.season,
    s.season_label,
    a.total_wins,
    a.total_losses,
    ROUND(a.win_pct, 3)                         AS win_pct,
    ROUND(a.net_rating, 2)                      AS net_rating,
    ROUND(a.avg_efg_pct * 100, 1)               AS efg_pct,
    ROUND(a.pace_estimate, 1)                   AS pace,
    CASE
        WHEN a.season BETWEEN 2014 AND 2018     THEN '🏆 Dynasty Era'
        WHEN a.season BETWEEN 2019 AND 2020     THEN '🤕 Injury Era'
        WHEN a.season >= 2021                   THEN '🔄 Rebuild / Return'
        ELSE                                         'Pre-Dynasty'
    END                                         AS era
FROM analytics_team_season a
JOIN dim_teams t  ON a.team_id = t.team_id
JOIN dim_season s ON a.season  = s.season
WHERE t.abbreviation = 'GSW'
ORDER BY a.season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q47. [TIME TREND] How has 3-point shooting evolved over 20 seasons?
--      Track league-wide 3P% and 3PA trends.
-- Skill: Aggregation over time, trend detection
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    season,
    ROUND(AVG(avg_fg3_pct) * 100, 2)            AS avg_league_3pt_pct,
    ROUND(MIN(avg_fg3_pct) * 100, 2)            AS min_3pt_pct,
    ROUND(MAX(avg_fg3_pct) * 100, 2)            AS max_3pt_pct,
    ROUND(AVG(avg_efg_pct) * 100, 2)            AS avg_efg_pct,
    COUNT(*)                                    AS teams_in_season,
    LAG(ROUND(AVG(avg_fg3_pct)*100,2)) OVER (ORDER BY season) AS prev_yr_3pt_pct,
    ROUND(
        AVG(avg_fg3_pct)*100 -
        LAG(AVG(avg_fg3_pct)*100) OVER (ORDER BY season), 2
    )                                           AS yoy_3pt_change
FROM analytics_team_season
GROUP BY season
ORDER BY season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q48. [TIME SERIES + CASE] Classify each season by its dominant style of play.
--      Use pace and scoring to characterize each era.
-- Skill: CASE on aggregated columns, era classification
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    season,
    ROUND(AVG(pace_estimate), 1)                AS avg_pace,
    ROUND(AVG(avg_pts_scored), 1)               AS avg_pts_per_team,
    ROUND(AVG(avg_fg3_pct) * 100, 1)            AS avg_3pt_pct,
    ROUND(AVG(net_rating), 2)                   AS avg_net_rating,
    CASE
        WHEN AVG(pace_estimate) >= 100
         AND AVG(avg_fg3_pct) >= 0.36           THEN '🚀 Modern 3-and-D Era'
        WHEN AVG(pace_estimate) >= 98            THEN '⚡ Up-tempo Era'
        WHEN AVG(pace_estimate) < 96             THEN '🧱 Defensive / Slow Era'
        ELSE                                         '⚖️  Transitional Era'
    END                                         AS playing_style
FROM analytics_team_season
GROUP BY season
ORDER BY season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q49. [TREND] Monthly performance breakdown for any team in any season.
--      (Parameterized query — swap team and season as needed)
-- Skill: Date grouping, multi-metric monthly breakdown
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    year_month,
    team,
    games,
    wins,
    (games - wins)                              AS losses,
    ROUND(wins * 1.0 / games, 3)                AS monthly_win_pct,
    ROUND(avg_pts_for, 1)                       AS avg_pts_scored,
    ROUND(avg_pts_against, 1)                   AS avg_pts_allowed,
    ROUND(avg_pts_for - avg_pts_against, 2)     AS monthly_net_rating,
    ROUND(avg_efg * 100, 1)                     AS efg_pct
FROM analytics_monthly_trends
WHERE season = 2022
  AND team = 'Boston Celtics'
ORDER BY year_month;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q50. [ADVANCED TIME SERIES] Find the single best 10-game stretch for any team
--      in the 2022-23 season (rolling 10-game win% peak).
-- Skill: ROW_NUMBER, self-join window, rolling stats simulation
-- ─────────────────────────────────────────────────────────────────────────────────
WITH game_results AS (
    SELECT
        g.game_date_est,
        g.season,
        ht.full_name                            AS team,
        ht.team_id,
        g.home_team_wins                        AS team_won,
        g.pts_home                              AS pts_scored,
        g.pts_away                              AS pts_allowed
    FROM fact_games g
    JOIN dim_teams ht ON g.home_team_id = ht.team_id
    WHERE g.pts_home IS NOT NULL
    UNION ALL
    SELECT
        g.game_date_est,
        g.season,
        at_.full_name,
        at_.team_id,
        1 - g.home_team_wins,
        g.pts_away,
        g.pts_home
    FROM fact_games g
    JOIN dim_teams at_ ON g.visitor_team_id = at_.team_id
    WHERE g.pts_away IS NOT NULL
),
numbered AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY team_id, season ORDER BY game_date_est) AS game_num
    FROM game_results
    WHERE season = 2022
)
SELECT
    team,
    game_num                                    AS game_window_end,
    game_num - 9                                AS game_window_start,
    SUM(team_won)                               AS wins_in_stretch,
    ROUND(AVG(pts_scored), 1)                   AS avg_pts_in_stretch,
    ROUND(AVG(pts_allowed), 1)                  AS avg_pts_allowed
FROM numbered n
WHERE game_num >= 10
  AND EXISTS (
      SELECT 1 FROM numbered n2
      WHERE n2.team_id = (SELECT team_id FROM dim_teams WHERE full_name = n.team)
        AND n2.season = 2022
        AND n2.game_num BETWEEN n.game_num - 9 AND n.game_num
      HAVING COUNT(*) = 10
  )
GROUP BY team, game_num
ORDER BY wins_in_stretch DESC, avg_pts_in_stretch DESC
LIMIT 10;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION G  |  HEAD-TO-HEAD & MATCHUP ANALYSIS  |  Q51–Q56
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q51. [JOIN + AGGREGATION] All-time head-to-head record between
--      Boston Celtics vs Los Angeles Lakers.
-- Skill: Multi-condition JOIN, CASE-based win counting
-- ─────────────────────────────────────────────────────────────────────────────────
WITH matchups AS (
    SELECT
        g.game_date_est,
        g.season,
        ht.full_name                            AS home_team,
        at_.full_name                           AS away_team,
        g.pts_home,
        g.pts_away,
        g.home_team_wins,
        CASE WHEN g.home_team_wins = 1 THEN ht.full_name ELSE at_.full_name END AS winner
    FROM fact_games g
    JOIN dim_teams ht  ON g.home_team_id     = ht.team_id
    JOIN dim_teams at_ ON g.visitor_team_id  = at_.team_id
    WHERE (ht.abbreviation = 'BOS' AND at_.abbreviation = 'LAL')
       OR (ht.abbreviation = 'LAL' AND at_.abbreviation = 'BOS')
      AND g.pts_home IS NOT NULL
)
SELECT
    COUNT(*)                                    AS total_games,
    SUM(CASE WHEN winner LIKE '%Celtics%' THEN 1 ELSE 0 END) AS celtics_wins,
    SUM(CASE WHEN winner LIKE '%Lakers%'  THEN 1 ELSE 0 END) AS lakers_wins,
    ROUND(AVG(pts_home + pts_away), 1)          AS avg_combined_score,
    ROUND(AVG(ABS(pts_home - pts_away)), 1)     AS avg_margin,
    MIN(game_date_est)                          AS first_matchup,
    MAX(game_date_est)                          AS latest_matchup
FROM matchups;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q52. [AGGREGATION] Build a rivalry matrix — win% for every team vs every
--      other team (top 5 teams, all-time).
-- Skill: Self-join, complex GROUP BY, competitive analysis
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    ht.abbreviation                             AS team,
    at_.abbreviation                            AS vs_team,
    COUNT(*)                                    AS games_played,
    SUM(g.home_team_wins)                       AS team_wins_as_home,
    ROUND(SUM(g.home_team_wins) * 1.0 / COUNT(*), 3) AS home_win_pct_vs
FROM fact_games g
JOIN dim_teams ht  ON g.home_team_id    = ht.team_id
JOIN dim_teams at_ ON g.visitor_team_id = at_.team_id
WHERE ht.abbreviation IN ('BOS','LAL','GSW','MIA','SAS')
  AND at_.abbreviation IN ('BOS','LAL','GSW','MIA','SAS')
  AND ht.abbreviation != at_.abbreviation
  AND g.pts_home IS NOT NULL
GROUP BY ht.abbreviation, at_.abbreviation
ORDER BY ht.abbreviation, home_win_pct_vs DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q53. [HEAD TO HEAD] Which team matchup produces the highest scoring games?
--      Rank all team combinations by average combined score.
-- Skill: Two-table JOIN as a composite key, ranking matchups
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    ht.abbreviation || ' vs ' || at_.abbreviation AS matchup,
    COUNT(*)                                    AS games_played,
    ROUND(AVG(g.pts_home + g.pts_away), 1)      AS avg_combined_score,
    MAX(g.pts_home + g.pts_away)                AS highest_combined_score,
    ROUND(AVG(ABS(g.pts_home - g.pts_away)), 1) AS avg_margin,
    SUM(g.home_team_wins)                       AS home_team_wins,
    COUNT(*) - SUM(g.home_team_wins)            AS away_team_wins
FROM fact_games g
JOIN dim_teams ht  ON g.home_team_id    = ht.team_id
JOIN dim_teams at_ ON g.visitor_team_id = at_.team_id
WHERE g.pts_home IS NOT NULL
GROUP BY ht.team_id, at_.team_id
HAVING games_played >= 10
ORDER BY avg_combined_score DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q54. [AGGREGATION] Find games decided by exactly 1 point — the closest games.
--      Which teams are involved most in nail-biters?
-- Skill: Filtering on derived column, aggregation on extreme values
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    ht.full_name                                AS home_team,
    at_.full_name                               AS away_team,
    g.game_date_est,
    g.season,
    g.pts_home,
    g.pts_away,
    ABS(g.pts_home - g.pts_away)                AS margin,
    CASE WHEN g.home_team_wins = 1 THEN ht.full_name ELSE at_.full_name END AS winner
FROM fact_games g
JOIN dim_teams ht  ON g.home_team_id    = ht.team_id
JOIN dim_teams at_ ON g.visitor_team_id = at_.team_id
WHERE ABS(g.pts_home - g.pts_away) <= 2
  AND g.pts_home IS NOT NULL
ORDER BY g.game_date_est DESC
LIMIT 30;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q55. [CTE] Find the team with the best record in games decided by ≤5 points
--      (clutch games — ability to win close games).
-- Skill: CTE + complex filtering + aggregation
-- ─────────────────────────────────────────────────────────────────────────────────
WITH clutch_games AS (
    SELECT
        g.season,
        ht.team_id                              AS home_id,
        at_.team_id                             AS away_id,
        ht.full_name                            AS home_name,
        at_.full_name                           AS away_name,
        g.home_team_wins,
        ABS(g.pts_home - g.pts_away)            AS margin
    FROM fact_games g
    JOIN dim_teams ht  ON g.home_team_id    = ht.team_id
    JOIN dim_teams at_ ON g.visitor_team_id = at_.team_id
    WHERE ABS(g.pts_home - g.pts_away) <= 5
      AND g.pts_home IS NOT NULL
),
clutch_record AS (
    SELECT home_id AS team_id, home_name AS team, home_team_wins AS won, season FROM clutch_games
    UNION ALL
    SELECT away_id, away_name, 1 - home_team_wins, season FROM clutch_games
)
SELECT
    team,
    SUM(won)                                    AS clutch_wins,
    COUNT(*) - SUM(won)                         AS clutch_losses,
    COUNT(*)                                    AS clutch_games_played,
    ROUND(SUM(won) * 1.0 / COUNT(*), 3)         AS clutch_win_pct
FROM clutch_record
GROUP BY team
ORDER BY clutch_win_pct DESC, clutch_wins DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q56. [JOIN] Which player performed best in head-to-head games between
--      the top 2 teams of 2022-23 (BOS vs MIL)?
-- Skill: Filtered player-game join, multi-table query
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    COUNT(DISTINCT p.game_id)                   AS games,
    ROUND(AVG(p.pts), 1)                        AS avg_pts,
    ROUND(AVG(p.reb), 1)                        AS avg_reb,
    ROUND(AVG(p.ast), 1)                        AS avg_ast,
    ROUND(AVG(p.plus_minus), 2)                 AS avg_plus_minus,
    ROUND(AVG(p.game_score), 2)                 AS avg_game_score
FROM fact_player_game_stats p
JOIN fact_games g ON p.game_id = g.game_id
JOIN dim_teams ht  ON g.home_team_id    = ht.team_id
JOIN dim_teams at_ ON g.visitor_team_id = at_.team_id
WHERE g.season = 2022
  AND p.team_abbreviation IN ('BOS', 'MIL')
  AND (
      (ht.abbreviation = 'BOS' AND at_.abbreviation = 'MIL') OR
      (ht.abbreviation = 'MIL' AND at_.abbreviation = 'BOS')
  )
  AND p.minutes_played >= 20
GROUP BY p.player_name, p.team_abbreviation
ORDER BY avg_game_score DESC;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION H  |  PLAYOFF & STANDINGS ANALYSIS  |  Q57–Q63
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q57. [AGGREGATION] Show the 2022-23 playoff picture — full standings by
--      conference with seeds, playoff status, and probability.
-- Skill: Filtering, multi-column ORDER BY, CASE classification
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    ap.conference,
    ap.conf_rank                                AS seed,
    ap.team,
    ap.wins,
    ap.losses,
    ROUND(ap.win_pct, 3)                        AS win_pct,
    ap.playoff_status,
    ap.playoff_probability_pct                  || '%' AS playoff_prob,
    -- Head-to-head tiebreaker indicator (simplified)
    CASE
        WHEN ap.conf_rank <= 4  THEN '✅ First Round Bye (historically)'
        WHEN ap.conf_rank <= 6  THEN '✅ Playoffs'
        WHEN ap.conf_rank <= 8  THEN '⚠️  Play-In Game'
        WHEN ap.conf_rank <= 10 THEN '🔶 Play-In Risk'
        ELSE                         '❌ Eliminated'
    END                                         AS bracket_position
FROM analytics_playoff_prob ap
WHERE ap.season = 2022
ORDER BY ap.conference, ap.conf_rank;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q58. [WINDOW + CTE] Which teams have been the most consistently elite?
--      Find teams in the top 3 of their conference the most times.
-- Skill: Window RANK within groups, aggregate counts of rank achievements
-- ─────────────────────────────────────────────────────────────────────────────────
WITH conf_top3 AS (
    SELECT
        team_id,
        team,
        conference,
        season,
        conf_rank,
        wins,
        win_pct
    FROM analytics_playoff_prob
    WHERE conf_rank <= 3
)
SELECT
    t.full_name                                 AS team,
    c.conference,
    COUNT(*)                                    AS top3_finishes,
    MIN(c.season)                               AS first_top3,
    MAX(c.season)                               AS last_top3,
    ROUND(AVG(c.win_pct), 3)                    AS avg_win_pct_in_top3,
    MAX(c.wins)                                 AS best_win_total
FROM conf_top3 c
JOIN dim_teams t ON c.team_id = t.team_id
GROUP BY c.team_id, t.full_name, c.conference
ORDER BY top3_finishes DESC, avg_win_pct_in_top3 DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q59. [SUBQUERY + FILTER] Which teams went from last place to playoffs
--      within 2 seasons? (Most improved franchises)
-- Skill: Correlated subquery, LAG, dramatic trend detection
-- ─────────────────────────────────────────────────────────────────────────────────
WITH season_records AS (
    SELECT
        team_id,
        full_name,
        season,
        total_wins,
        win_pct,
        RANK() OVER (PARTITION BY season ORDER BY win_pct) AS league_rank,
        LAG(win_pct) OVER (PARTITION BY team_id ORDER BY season) AS prev_win_pct,
        LAG(total_wins) OVER (PARTITION BY team_id ORDER BY season) AS prev_wins
    FROM analytics_team_season
)
SELECT
    full_name                                   AS team,
    season,
    total_wins,
    ROUND(win_pct, 3)                           AS current_win_pct,
    prev_wins,
    ROUND(prev_win_pct, 3)                      AS prev_win_pct,
    (total_wins - prev_wins)                    AS wins_gained,
    ROUND(win_pct - prev_win_pct, 3)            AS win_pct_improvement
FROM season_records
WHERE total_wins - prev_wins >= 15
  AND prev_wins IS NOT NULL
ORDER BY wins_gained DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q60. [AGGREGATION] What is the historical win% of the #1 seed vs all seeds?
--      Show each conference seed's championship probability proxy.
-- Skill: GROUP BY on ranking column, win% by seed
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    conf_rank                                   AS conference_seed,
    COUNT(*)                                    AS team_seasons,
    ROUND(AVG(wins), 1)                         AS avg_wins,
    ROUND(AVG(win_pct), 3)                      AS avg_win_pct,
    MAX(wins)                                   AS best_wins,
    MIN(wins)                                   AS lowest_wins_at_this_seed,
    CASE conf_rank
        WHEN 1  THEN '~20% championship odds'
        WHEN 2  THEN '~15% championship odds'
        WHEN 3  THEN '~10% championship odds'
        WHEN 4  THEN '~8%  championship odds'
        WHEN 5  THEN '~6%  championship odds'
        WHEN 6  THEN '~4%  championship odds'
        WHEN 7  THEN '~2%  championship odds'
        WHEN 8  THEN '~1%  championship odds'
        ELSE         '<1% championship odds'
    END                                         AS historical_champ_odds
FROM analytics_playoff_prob
WHERE season >= 2010
GROUP BY conf_rank
ORDER BY conf_rank;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q61. [CTE] Which team had the biggest single-season collapse?
--      (Highest wins one season → lowest wins the next)
-- Skill: LAG, MAX over partition, dramatic negative trend
-- ─────────────────────────────────────────────────────────────────────────────────
WITH changes AS (
    SELECT
        full_name                               AS team,
        season,
        total_wins,
        LAG(total_wins) OVER (PARTITION BY team_id ORDER BY season) AS prev_season_wins,
        total_wins - LAG(total_wins) OVER (PARTITION BY team_id ORDER BY season) AS win_change
    FROM analytics_team_season
)
SELECT
    team,
    season,
    prev_season_wins                            AS wins_prior_season,
    total_wins                                  AS wins_this_season,
    win_change,
    CASE
        WHEN win_change <= -15               THEN '💥 Catastrophic Collapse'
        WHEN win_change <= -10              THEN '📉 Major Decline'
        WHEN win_change >= 15               THEN '🚀 Dramatic Rise'
        WHEN win_change >= 10               THEN '📈 Strong Improvement'
    END                                         AS event_label
FROM changes
WHERE ABS(win_change) >= 10
  AND prev_season_wins IS NOT NULL
ORDER BY win_change ASC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q62. [ADVANCED] Predict playoff probability based on win% using a tiered model.
--      Show the entire current standings with live probability estimates.
-- Skill: CASE-based probability model, ranking + business logic
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    conference,
    conf_rank                                   AS seed,
    team,
    wins,
    losses,
    ROUND(win_pct, 3)                           AS win_pct,
    -- Tiered probability model
    CASE
        WHEN win_pct >= 0.70                    THEN 99
        WHEN win_pct >= 0.65                    THEN 97
        WHEN win_pct >= 0.60                    THEN 93
        WHEN win_pct >= 0.55                    THEN 80
        WHEN win_pct >= 0.50                    THEN 65
        WHEN win_pct >= 0.47                    THEN 50
        WHEN win_pct >= 0.44                    THEN 35
        WHEN win_pct >= 0.40                    THEN 20
        WHEN win_pct >= 0.35                    THEN 10
        ELSE                                         3
    END                                         AS model_playoff_pct,
    playoff_probability_pct                     AS stored_playoff_pct,
    playoff_status
FROM analytics_playoff_prob
WHERE season = 2022
ORDER BY conference, conf_rank;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q63. [AGGREGATION] Conference dominance analysis — East vs West comparison
--      across all seasons.
-- Skill: GROUP BY on categorical column, cross-conference comparison
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    season,
    conference,
    COUNT(*)                                    AS teams,
    ROUND(AVG(win_pct), 3)                      AS avg_win_pct,
    MAX(wins)                                   AS best_record_wins,
    MIN(wins)                                   AS worst_record_wins,
    -- Top-seed team
    MAX(CASE WHEN conf_rank = 1 THEN team END)  AS conference_leader,
    MAX(CASE WHEN conf_rank = 1 THEN wins  END) AS leader_wins
FROM analytics_playoff_prob
WHERE season >= 2015
GROUP BY season, conference
ORDER BY season DESC, conference;


-- ══════════════════════════════════════════════════════════════════════════════════
--  SECTION I  |  BUSINESS / INTERVIEW QUESTIONS  |  Q64–Q75
-- ══════════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────────
-- Q64. [INTERVIEW CLASSIC] Find the Nth highest average scoring team
--      in the 2022-23 season (parameterize N = 5).
-- Skill: LIMIT + OFFSET or subquery method — a top-3 interview question.
-- ─────────────────────────────────────────────────────────────────────────────────
-- Method 1: LIMIT + OFFSET
SELECT full_name AS team, ROUND(avg_pts_scored, 1) AS avg_pts
FROM analytics_team_season
WHERE season = 2022
ORDER BY avg_pts_scored DESC
LIMIT 1 OFFSET 4;  -- 0-indexed: 4 = 5th highest

-- Method 2: Subquery (works in all SQL dialects)
SELECT full_name AS team, ROUND(avg_pts_scored, 1) AS avg_pts
FROM analytics_team_season
WHERE season = 2022
  AND avg_pts_scored = (
      SELECT DISTINCT avg_pts_scored
      FROM analytics_team_season
      WHERE season = 2022
      ORDER BY avg_pts_scored DESC
      LIMIT 1 OFFSET 4
  );


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q65. [INTERVIEW CLASSIC] Find duplicate records — are there any games played
--      on the same date between the same two teams (data quality check)?
-- Skill: Self-join or GROUP BY + HAVING COUNT > 1
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    game_date_est,
    home_team_id,
    visitor_team_id,
    COUNT(*)                                    AS occurrences
FROM fact_games
GROUP BY game_date_est, home_team_id, visitor_team_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
-- (Should return 0 rows — confirms data integrity)


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q66. [INTERVIEW] What percentage of games are won by the home team
--      each season? Has home court advantage declined over time?
-- Skill: Percentage calculation, GROUP BY year, trend analysis
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    season,
    COUNT(*)                                    AS total_games,
    SUM(home_team_wins)                         AS home_wins,
    COUNT(*) - SUM(home_team_wins)              AS away_wins,
    ROUND(SUM(home_team_wins) * 100.0 / COUNT(*), 1) AS home_win_pct,
    ROUND((COUNT(*) - SUM(home_team_wins)) * 100.0 / COUNT(*), 1) AS away_win_pct,
    LAG(ROUND(SUM(home_team_wins)*100.0/COUNT(*),1))
        OVER (ORDER BY season)                  AS prev_yr_home_win_pct
FROM fact_games
WHERE pts_home IS NOT NULL
GROUP BY season
ORDER BY season;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q67. [INTERVIEW] Using only SQL, calculate the median points scored
--      by the Boston Celtics in 2022-23.
-- Skill: Median calculation in SQL (no built-in function) — HARD interview question
-- ─────────────────────────────────────────────────────────────────────────────────
WITH bos_scores AS (
    SELECT pts, ROW_NUMBER() OVER (ORDER BY pts) AS rn, COUNT(*) OVER () AS total
    FROM (
        SELECT pts_home AS pts FROM fact_games g
        JOIN dim_teams t ON g.home_team_id = t.team_id
        WHERE t.abbreviation = 'BOS' AND g.season = 2022 AND g.pts_home IS NOT NULL
        UNION ALL
        SELECT pts_away FROM fact_games g
        JOIN dim_teams t ON g.visitor_team_id = t.team_id
        WHERE t.abbreviation = 'BOS' AND g.season = 2022 AND g.pts_away IS NOT NULL
    ) sub
)
SELECT
    ROUND(AVG(pts), 1)                          AS median_pts
FROM bos_scores
WHERE rn IN (
    (total + 1) / 2,
    (total + 2) / 2
);


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q68. [INTERVIEW] Identify "outlier" performances — games where a player
--      scored more than 2.5 standard deviations above their season average.
-- Skill: Statistical outlier detection, derived metrics, joins
-- Why asked: Outlier detection is a common analyst task in any industry.
-- ─────────────────────────────────────────────────────────────────────────────────
WITH player_season_stats AS (
    SELECT
        p.player_id,
        p.player_name,
        p.team_abbreviation,
        g.season,
        AVG(p.pts)                              AS avg_pts,
        SQRT(AVG(p.pts*p.pts) - AVG(p.pts)*AVG(p.pts)) AS std_pts
    FROM fact_player_game_stats p
    JOIN fact_games g ON p.game_id = g.game_id
    WHERE g.season = 2022 AND p.minutes_played >= 10
    GROUP BY p.player_id, p.player_name, p.team_abbreviation, g.season
    HAVING COUNT(*) >= 20
)
SELECT
    p.player_name,
    p.team_abbreviation                         AS team,
    g.game_date_est,
    p.pts                                       AS game_pts,
    ROUND(pss.avg_pts, 1)                       AS season_avg_pts,
    ROUND(pss.std_pts, 2)                       AS std_dev,
    ROUND((p.pts - pss.avg_pts) / NULLIF(pss.std_pts, 0), 2) AS z_score
FROM fact_player_game_stats p
JOIN fact_games g          ON p.game_id   = g.game_id
JOIN player_season_stats pss ON p.player_id = pss.player_id
                            AND g.season    = pss.season
WHERE g.season = 2022
  AND p.pts > pss.avg_pts + 2.5 * pss.std_pts
ORDER BY z_score DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q69. [INTERVIEW — BUSINESS] If you were advising an NBA team's GM,
--      which undervalued players (high Game Score, low PPG notoriety)
--      would you target as free agents?
-- Skill: Multi-metric filter, ratio analysis, business framing of data
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    player_name,
    team_abbreviation                           AS team,
    games_played,
    ROUND(avg_pts, 1)                           AS ppg,
    ROUND(avg_reb, 1)                           AS rpg,
    ROUND(avg_ast, 1)                           AS apg,
    ROUND(avg_game_score, 2)                    AS game_score,
    ROUND(avg_plus_minus, 2)                    AS plus_minus,
    -- Efficiency ratio: game score vs points (high = efficient, low scorer)
    ROUND(avg_game_score / NULLIF(avg_pts, 0), 3) AS efficiency_per_pt,
    CASE
        WHEN avg_pts < 18
         AND avg_game_score > 14               THEN '💎 Hidden Gem'
        WHEN avg_pts < 15
         AND avg_plus_minus > 3                THEN '🔍 Underrated Role Player'
        WHEN avg_pts BETWEEN 15 AND 20
         AND avg_game_score > 15               THEN '📈 Breakout Candidate'
        ELSE                                        'Standard'
    END                                         AS scout_label
FROM analytics_player_season
WHERE season = 2022
  AND games_played >= 30
  AND avg_game_score > 12
  AND avg_pts < 20
ORDER BY avg_game_score DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q70. [INTERVIEW — PIVOT] Pivot team performance data: show each team's wins
--      for 2019, 2020, 2021, 2022 as columns.
-- Skill: CASE-based pivot / cross-tab — very common BI interview question
-- ─────────────────────────────────────────────────────────────────────────────────
SELECT
    t.full_name                                 AS team,
    t.abbreviation,
    MAX(CASE WHEN a.season = 2019 THEN a.total_wins END) AS wins_2019,
    MAX(CASE WHEN a.season = 2020 THEN a.total_wins END) AS wins_2020,
    MAX(CASE WHEN a.season = 2021 THEN a.total_wins END) AS wins_2021,
    MAX(CASE WHEN a.season = 2022 THEN a.total_wins END) AS wins_2022,
    -- Net change
    MAX(CASE WHEN a.season = 2022 THEN a.total_wins END) -
    MAX(CASE WHEN a.season = 2019 THEN a.total_wins END) AS wins_change_3yr
FROM analytics_team_season a
JOIN dim_teams t ON a.team_id = t.team_id
WHERE a.season IN (2019, 2020, 2021, 2022)
GROUP BY t.full_name, t.abbreviation
ORDER BY wins_2022 DESC NULLS LAST;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q71. [INTERVIEW] Find teams that won MORE games away than at home in a season.
--      (Counter-intuitive result — home court disadvantage)
-- Skill: CTEs, comparison of home vs away wins, anomaly detection
-- ─────────────────────────────────────────────────────────────────────────────────
WITH records AS (
    SELECT g.season, g.home_team_id AS tid, SUM(g.home_team_wins) AS hw, COUNT(*) AS hg FROM fact_games g WHERE pts_home IS NOT NULL GROUP BY g.season, g.home_team_id
    -- home record
),
away_rec AS (
    SELECT g.season, g.visitor_team_id AS tid, SUM(1-g.home_team_wins) AS aw, COUNT(*) AS ag FROM fact_games g WHERE pts_away IS NOT NULL GROUP BY g.season, g.visitor_team_id
    -- away record
)
SELECT
    t.full_name                                 AS team,
    r.season,
    r.hw                                        AS home_wins,
    r.hg - r.hw                                 AS home_losses,
    a.aw                                        AS away_wins,
    a.ag - a.aw                                 AS away_losses,
    ROUND(r.hw * 1.0 / r.hg, 3)                AS home_win_pct,
    ROUND(a.aw * 1.0 / a.ag, 3)                AS away_win_pct,
    ROUND(a.aw * 1.0/a.ag - r.hw * 1.0/r.hg, 3) AS away_minus_home_pct
FROM records r
JOIN away_rec a  ON r.tid = a.tid AND r.season = a.season
JOIN dim_teams t ON r.tid = t.team_id
WHERE a.aw > r.hw   -- More away wins than home wins
ORDER BY away_minus_home_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q72. [INTERVIEW — COHORT] Build a player retention / longevity analysis.
--      How many players appeared in 3+ consecutive seasons with the same team?
-- Skill: Self-join for sequential seasons, loyalty/retention analysis
-- ─────────────────────────────────────────────────────────────────────────────────
WITH player_seasons AS (
    SELECT DISTINCT player_name, team_abbreviation, season
    FROM analytics_player_season
    WHERE games_played >= 20
),
consecutive AS (
    SELECT
        a.player_name,
        a.team_abbreviation                     AS team,
        a.season                                AS season1,
        b.season                                AS season2,
        c.season                                AS season3
    FROM player_seasons a
    JOIN player_seasons b ON a.player_name = b.player_name
                         AND a.team_abbreviation = b.team_abbreviation
                         AND b.season = a.season + 1
    JOIN player_seasons c ON a.player_name = c.player_name
                         AND a.team_abbreviation = c.team_abbreviation
                         AND c.season = a.season + 2
)
SELECT
    player_name,
    team,
    COUNT(*)                                    AS consecutive_3yr_streaks,
    MIN(season1)                                AS earliest_streak_start,
    MAX(season3)                                AS latest_streak_end
FROM consecutive
GROUP BY player_name, team
ORDER BY consecutive_3yr_streaks DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q73. [INTERVIEW — SEGMENTATION] RFM-style player segmentation.
--      Segment players by: Recency (last season), Frequency (seasons played),
--      Magnitude (career average PPG).
-- Skill: RFM framework applied to sports — demonstrates marketing analytics crossover
-- ─────────────────────────────────────────────────────────────────────────────────
WITH player_rfm AS (
    SELECT
        player_name,
        MAX(season)                             AS last_season,     -- Recency
        COUNT(DISTINCT season)                  AS seasons_active,   -- Frequency
        ROUND(AVG(avg_pts), 1)                  AS career_avg_ppg,   -- Magnitude
        SUM(total_pts)                          AS career_pts,
        ROUND(AVG(avg_game_score), 2)           AS career_game_score
    FROM analytics_player_season
    WHERE games_played >= 20
    GROUP BY player_name
)
SELECT
    player_name,
    last_season,
    seasons_active,
    career_avg_ppg,
    career_pts,
    career_game_score,
    -- RFM Segments
    CASE WHEN last_season >= 2021 THEN 'Recent' ELSE 'Legacy' END AS recency_segment,
    CASE
        WHEN seasons_active >= 8    THEN 'Veteran (8+ seasons)'
        WHEN seasons_active >= 4    THEN 'Established (4-7)'
        ELSE                             'Young (<4 seasons)'
    END                                         AS tenure_segment,
    CASE
        WHEN career_avg_ppg >= 20   THEN '⭐ Star Scorer'
        WHEN career_avg_ppg >= 12   THEN '💪 Solid Contributor'
        ELSE                             '🔧 Role Player'
    END                                         AS impact_segment
FROM player_rfm
ORDER BY career_avg_ppg DESC
LIMIT 30;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q74. [INTERVIEW — ADVANCED CTE CHAIN] Multi-step CTE pipeline:
--      Step 1: Get each team's average stats
--      Step 2: Compare to league averages
--      Step 3: Classify each team's strengths and weaknesses
-- Skill: Chained CTEs — the most important SQL pattern for analytics
-- ─────────────────────────────────────────────────────────────────────────────────
WITH team_stats AS (
    -- Step 1: Team averages
    SELECT team_id, full_name, abbreviation, season,
           avg_pts_scored, avg_pts_allowed, avg_fg3_pct,
           avg_efg_pct, net_rating, win_pct, total_wins
    FROM analytics_team_season
    WHERE season = 2022
),
league_avgs AS (
    -- Step 2: League-wide averages
    SELECT
        AVG(avg_pts_scored)     AS lg_avg_pts,
        AVG(avg_pts_allowed)    AS lg_avg_pts_allowed,
        AVG(avg_fg3_pct)        AS lg_avg_3pt,
        AVG(avg_efg_pct)        AS lg_avg_efg,
        AVG(net_rating)         AS lg_avg_net_rating
    FROM team_stats
),
comparison AS (
    -- Step 3: Compare each team vs league
    SELECT
        ts.full_name                            AS team,
        ts.abbreviation,
        ts.total_wins,
        ROUND(ts.win_pct, 3)                    AS win_pct,
        ROUND(ts.avg_pts_scored - la.lg_avg_pts, 1)          AS vs_lg_offense,
        ROUND(ts.avg_pts_allowed - la.lg_avg_pts_allowed, 1) AS vs_lg_defense,
        ROUND((ts.avg_efg_pct - la.lg_avg_efg) * 100, 2)     AS vs_lg_efg,
        ROUND(ts.net_rating - la.lg_avg_net_rating, 2)        AS vs_lg_net_rating
    FROM team_stats ts
    CROSS JOIN league_avgs la
)
-- Final output with team profile
SELECT
    team,
    abbreviation,
    total_wins,
    win_pct,
    vs_lg_offense,
    vs_lg_defense,
    vs_lg_efg,
    vs_lg_net_rating,
    CASE
        WHEN vs_lg_offense > 3 AND vs_lg_defense < -2 THEN '🚀 Two-Way Powerhouse'
        WHEN vs_lg_offense > 3 AND vs_lg_defense >= 0 THEN '🏹 Offensive Powerhouse'
        WHEN vs_lg_defense < -3 AND vs_lg_offense <= 0 THEN '🛡️  Defensive Fortress'
        WHEN vs_lg_net_rating > 2                      THEN '⚖️  Well-Balanced'
        WHEN vs_lg_net_rating < -3                     THEN '⬇️  Below Average'
        ELSE                                                '🔄 Average Team'
    END                                         AS team_profile
FROM comparison
ORDER BY total_wins DESC;


-- ─────────────────────────────────────────────────────────────────────────────────
-- Q75. [CAPSTONE] Full Executive Summary Query — single query that delivers
--      the complete season snapshot: standings, top scorer, best team, trends.
-- Skill: Multiple CTEs, complex joins, business-ready output
-- This is the kind of query you'd actually run for a CEO/executive report.
-- ─────────────────────────────────────────────────────────────────────────────────
WITH season_summary AS (
    SELECT
        season,
        COUNT(DISTINCT team_id)                 AS teams,
        SUM(total_wins) / 2                     AS total_games_played,
        ROUND(AVG(avg_pts_scored), 1)           AS league_avg_pts,
        ROUND(AVG(avg_efg_pct) * 100, 1)        AS league_efg_pct,
        ROUND(AVG(net_rating), 2)               AS league_avg_net_rating,
        ROUND(AVG(pace_estimate), 1)            AS league_pace
    FROM analytics_team_season
    WHERE season = 2022
    GROUP BY season
),
best_team AS (
    SELECT full_name AS best_team_name, total_wins AS best_team_wins, ROUND(net_rating,2) AS best_net_rating
    FROM analytics_team_season WHERE season = 2022 ORDER BY win_pct DESC LIMIT 1
),
top_scorer AS (
    SELECT player_name AS top_scorer_name, team_abbreviation AS top_scorer_team, ROUND(avg_pts,1) AS top_scorer_ppg
    FROM analytics_player_season WHERE season = 2022 AND games_played >= 20 ORDER BY avg_pts DESC LIMIT 1
),
most_efficient AS (
    SELECT player_name AS efficient_player, ROUND(avg_game_score,2) AS top_game_score
    FROM analytics_player_season WHERE season = 2022 AND games_played >= 20 ORDER BY avg_game_score DESC LIMIT 1
)
SELECT
    '2022-23 NBA Season'                        AS season_label,
    ss.teams,
    ss.total_games_played,
    ss.league_avg_pts                           AS avg_pts_per_team,
    ss.league_efg_pct                           AS league_efg_pct,
    ss.league_pace                              AS avg_pace,
    bt.best_team_name                           AS best_team,
    bt.best_team_wins,
    bt.best_net_rating,
    ts.top_scorer_name                          AS scoring_leader,
    ts.top_scorer_team,
    ts.top_scorer_ppg,
    me.efficient_player                         AS most_efficient_player,
    me.top_game_score
FROM season_summary ss
CROSS JOIN best_team bt
CROSS JOIN top_scorer ts
CROSS JOIN most_efficient me;


-- ══════════════════════════════════════════════════════════════════════════════════
--  END OF PROJECT
-- ══════════════════════════════════════════════════════════════════════════════════
-- ✅  75 Questions Covered
-- ✅  Skills: SELECT · JOIN · GROUP BY · HAVING · WHERE · CASE WHEN
--            Subqueries · CTEs · Window Functions (RANK/LAG/LEAD/NTILE/
--            PERCENT_RANK/CUME_DIST/RUNNING TOTAL/MOVING AVERAGE)
--            Date Functions · NULL Handling · Data Profiling
--            Advanced Metrics (eFG%/Net Rating/PER/BPM/TS%/Win Shares)
--            Business Analytics · RFM · Outlier Detection · Pivot Queries
-- ══════════════════════════════════════════════════════════════════════════════════
