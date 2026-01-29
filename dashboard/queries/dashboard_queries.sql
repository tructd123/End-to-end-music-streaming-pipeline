-- =====================================================
-- SOUNDFLOW DASHBOARD QUERIES
-- Ready-to-use queries for Looker Studio / Metabase
-- =====================================================

-- =====================================================
-- 1. EXECUTIVE OVERVIEW QUERIES
-- =====================================================

-- 1.1 Today's KPIs
SELECT
    event_date,
    total_plays,
    unique_users,
    unique_songs,
    unique_artists,
    total_sessions,
    paid_plays_pct,
    plays_per_user
FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary`
WHERE event_date = CURRENT_DATE()
;

-- 1.2 Weekly Comparison
SELECT
    event_date,
    total_plays,
    unique_users,
    LAG(total_plays, 7) OVER (ORDER BY event_date) as same_day_last_week,
    ROUND((total_plays - LAG(total_plays, 7) OVER (ORDER BY event_date)) * 100.0 
          / NULLIF(LAG(total_plays, 7) OVER (ORDER BY event_date), 0), 2) as wow_growth_pct
FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
ORDER BY event_date DESC
;

-- 1.3 Monthly Summary
SELECT
    FORMAT_DATE('%Y-%m', event_date) as month,
    SUM(total_plays) as total_plays,
    SUM(unique_users) as total_unique_users,
    AVG(paid_plays_pct) as avg_paid_pct,
    SUM(total_sessions) as total_sessions
FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary`
GROUP BY FORMAT_DATE('%Y-%m', event_date)
ORDER BY month DESC
;


-- =====================================================
-- 2. MUSIC ANALYTICS QUERIES
-- =====================================================

-- 2.1 Top 10 Songs with Details
SELECT
    rank,
    song,
    artist,
    total_plays,
    unique_listeners,
    unique_sessions,
    paid_plays,
    free_plays,
    ROUND(paid_ratio_pct, 1) as paid_pct,
    plays_per_listener,
    peak_time_of_day
FROM `graphic-boulder-483814-g7.staging_marts.mart_top_songs`
WHERE rank <= 10
ORDER BY rank
;

-- 2.2 Top Artists
SELECT
    artist,
    total_plays,
    unique_songs,
    unique_listeners,
    ROUND(avg_plays_per_song, 2) as avg_plays_per_song,
    most_played_song
FROM `graphic-boulder-483814-g7.staging_marts.mart_top_artists`
ORDER BY total_plays DESC
LIMIT 20
;

-- 2.3 Song Trends Over Time
SELECT
    h.event_date,
    h.hour_of_day,
    h.hourly_plays,
    ts.song,
    ts.artist
FROM `graphic-boulder-483814-g7.staging_marts.mart_hourly_metrics` h
CROSS JOIN (
    SELECT song, artist 
    FROM `graphic-boulder-483814-g7.staging_marts.mart_top_songs`
    WHERE rank <= 5
) ts
WHERE h.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY h.event_date, h.hour_of_day
;

-- 2.4 Peak Hours Analysis
SELECT
    hour_of_day,
    CASE 
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour_of_day BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END as time_period,
    AVG(hourly_plays) as avg_plays,
    AVG(active_users_in_hour) as avg_active_users,
    AVG(unique_songs_in_hour) as avg_unique_songs
FROM `graphic-boulder-483814-g7.staging_marts.mart_hourly_metrics`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY hour_of_day
ORDER BY hour_of_day
;


-- =====================================================
-- 3. USER ENGAGEMENT QUERIES
-- =====================================================

-- 3.1 Engagement Tier Distribution
SELECT
    engagement_tier,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    AVG(total_plays) as avg_plays,
    AVG(active_days) as avg_active_days
FROM `graphic-boulder-483814-g7.staging_marts.mart_active_users`
GROUP BY engagement_tier
ORDER BY 
    CASE engagement_tier 
        WHEN 'Power User' THEN 1 
        WHEN 'Active' THEN 2 
        WHEN 'Casual' THEN 3 
        WHEN 'New' THEN 4 
    END
;

-- 3.2 Paid vs Free Users
SELECT
    current_level,
    COUNT(*) as user_count,
    AVG(total_plays) as avg_plays,
    AVG(unique_songs) as avg_unique_songs,
    AVG(active_days) as avg_active_days
FROM `graphic-boulder-483814-g7.staging_marts.mart_active_users`
GROUP BY current_level
;

-- 3.3 User Activity Heatmap Data
SELECT
    favorite_time,
    preferred_days,
    COUNT(*) as user_count,
    AVG(total_plays) as avg_plays
FROM `graphic-boulder-483814-g7.staging_marts.mart_active_users`
WHERE favorite_time IS NOT NULL
GROUP BY favorite_time, preferred_days
ORDER BY favorite_time, preferred_days
;

-- 3.4 Top Power Users
SELECT
    user_id,
    full_name,
    current_level,
    total_plays,
    unique_songs,
    unique_artists,
    active_days,
    engagement_tier,
    song_diversity_pct
FROM `graphic-boulder-483814-g7.staging_marts.mart_active_users`
WHERE engagement_tier = 'Power User'
ORDER BY total_plays DESC
LIMIT 50
;


-- =====================================================
-- 4. GEOGRAPHIC ANALYTICS QUERIES
-- =====================================================

-- 4.1 Plays by State (US Map)
SELECT
    state,
    total_plays,
    unique_users,
    ROUND(total_plays / NULLIF(unique_users, 0), 2) as plays_per_user
FROM `graphic-boulder-483814-g7.staging_marts.mart_location_analytics`
ORDER BY total_plays DESC
;

-- 4.2 Top Cities
SELECT
    city,
    state,
    total_plays,
    unique_users
FROM `graphic-boulder-483814-g7.staging_marts.mart_location_analytics`
ORDER BY total_plays DESC
LIMIT 20
;


-- =====================================================
-- 5. REAL-TIME MONITORING QUERIES
-- =====================================================

-- 5.1 Today's Hourly Progress
SELECT
    hour_of_day,
    hourly_plays,
    active_users_in_hour,
    cumulative_plays,
    ROUND(cumulative_plays * 100.0 / MAX(cumulative_plays) OVER (), 2) as daily_progress_pct
FROM `graphic-boulder-483814-g7.staging_marts.mart_hourly_metrics`
WHERE event_date = CURRENT_DATE()
ORDER BY hour_of_day
;

-- 5.2 Hourly Comparison (Today vs Yesterday)
SELECT
    t.hour_of_day,
    COALESCE(t.hourly_plays, 0) as today_plays,
    COALESCE(y.hourly_plays, 0) as yesterday_plays,
    COALESCE(t.hourly_plays, 0) - COALESCE(y.hourly_plays, 0) as diff,
    CASE 
        WHEN COALESCE(y.hourly_plays, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(t.hourly_plays, 0) - COALESCE(y.hourly_plays, 0)) * 100.0 / y.hourly_plays, 2)
    END as change_pct
FROM (
    SELECT * FROM `graphic-boulder-483814-g7.staging_marts.mart_hourly_metrics`
    WHERE event_date = CURRENT_DATE()
) t
FULL OUTER JOIN (
    SELECT * FROM `graphic-boulder-483814-g7.staging_marts.mart_hourly_metrics`
    WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
) y ON t.hour_of_day = y.hour_of_day
ORDER BY COALESCE(t.hour_of_day, y.hour_of_day)
;

-- 5.3 Rolling 7-Day Average
SELECT
    event_date,
    total_plays,
    AVG(total_plays) OVER (
        ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_avg
FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY event_date
;


-- =====================================================
-- 6. DASHBOARD SCORECARD QUERIES
-- =====================================================

-- 6.1 Quick Stats (Single Row for Scorecards)
SELECT
    -- Today's stats
    (SELECT total_plays FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary` 
     WHERE event_date = CURRENT_DATE()) as plays_today,
    
    (SELECT unique_users FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary` 
     WHERE event_date = CURRENT_DATE()) as users_today,
    
    -- Total stats
    (SELECT COUNT(*) FROM `graphic-boulder-483814-g7.staging_marts.mart_active_users`) as total_users,
    
    (SELECT COUNT(*) FROM `graphic-boulder-483814-g7.staging_marts.mart_top_songs`) as catalog_size,
    
    -- Growth
    (SELECT 
        ROUND((a.total_plays - b.total_plays) * 100.0 / NULLIF(b.total_plays, 0), 2)
     FROM `graphic-boulder-483814-g7.staging_marts.mart_daily_summary` a
     JOIN `graphic-boulder-483814-g7.staging_marts.mart_daily_summary` b
       ON a.event_date = CURRENT_DATE() AND b.event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) as daily_growth_pct
;
