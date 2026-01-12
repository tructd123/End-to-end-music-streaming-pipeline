-- ===============================================
-- ADMINER QUERY EXAMPLES FOR SOUNDFLOW
-- Copy và paste các query này vào Adminer UI
-- ===============================================

-- 1. Xem 10 bài hát được nghe gần nhất
SELECT 
    event_timestamp,
    first_name,
    last_name,
    song,
    artist,
    city,
    state,
    level
FROM raw.listen_events
ORDER BY event_timestamp DESC
LIMIT 10;

-- 2. Đếm tổng số events theo loại
SELECT 
    'listen_events' as table_name, 
    COUNT(*) as total
FROM raw.listen_events
UNION ALL
SELECT 'page_view_events', COUNT(*)
FROM raw.page_view_events
UNION ALL
SELECT 'auth_events', COUNT(*)
FROM raw.auth_events
UNION ALL
SELECT 'status_change_events', COUNT(*)
FROM raw.status_change_events;

-- 3. Top 10 bài hát phổ biến nhất
SELECT 
    song,
    artist,
    COUNT(*) as play_count
FROM raw.listen_events
GROUP BY song, artist
ORDER BY play_count DESC
LIMIT 10;

-- 4. Top 10 users nghe nhạc nhiều nhất
SELECT 
    user_id,
    first_name,
    last_name,
    COUNT(*) as total_plays,
    COUNT(DISTINCT song) as unique_songs
FROM raw.listen_events
GROUP BY user_id, first_name, last_name
ORDER BY total_plays DESC
LIMIT 10;

-- 5. Phân bố users theo thành phố
SELECT 
    city,
    state,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as total_plays
FROM raw.listen_events
WHERE city IS NOT NULL
GROUP BY city, state
ORDER BY unique_users DESC
LIMIT 15;

-- 6. Phân tích theo level (free vs paid)
SELECT 
    level,
    COUNT(*) as play_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT song) as unique_songs
FROM raw.listen_events
GROUP BY level;

-- 7. Activity theo giờ trong ngày
SELECT 
    EXTRACT(HOUR FROM event_timestamp) as hour_of_day,
    COUNT(*) as event_count
FROM raw.listen_events
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 8. Tìm users chuyển từ free sang paid
SELECT 
    user_id,
    first_name,
    last_name,
    new_level,
    event_timestamp
FROM raw.status_change_events
ORDER BY event_timestamp DESC;

-- 9. Top nghệ sĩ được nghe nhiều nhất
SELECT 
    artist,
    COUNT(*) as play_count,
    COUNT(DISTINCT song) as unique_songs,
    COUNT(DISTINCT user_id) as unique_listeners
FROM raw.listen_events
GROUP BY artist
ORDER BY play_count DESC
LIMIT 10;

-- 10. Kiểm tra data quality - tìm records có missing data
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN city IS NULL THEN 1 END) as missing_city,
    COUNT(CASE WHEN state IS NULL THEN 1 END) as missing_state,
    COUNT(CASE WHEN song IS NULL THEN 1 END) as missing_song
FROM raw.listen_events;
