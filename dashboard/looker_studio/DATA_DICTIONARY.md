# SoundFlow Data Dictionary

Tài liệu mô tả chi tiết các cột trong các bảng Mart Data dùng cho Dashboard.

---

## 📊 Tổng quan các Mart Tables

| Table | Mô tả | Materialization | Use Case |
|-------|-------|-----------------|----------|
| `mart_daily_summary` | Tổng hợp KPIs theo ngày | Incremental | Executive Dashboard |
| `mart_hourly_metrics` | Metrics theo giờ | Incremental | Time-series Analysis |
| `mart_active_users` | Thông tin user engagement | Table | User Analytics |
| `mart_top_songs` | Top 100 bài hát | Table | Content Performance |
| `mart_top_artists` | Top 100 nghệ sĩ | Table | Artist Analytics |
| `mart_location_analytics` | Phân tích theo địa lý | Incremental | Regional Insights |

---

## 1. mart_daily_summary

**Mô tả**: Tổng hợp KPIs hàng ngày cho Executive Dashboard

**Unique Key**: `event_date`

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `event_date` | DATE | Ngày thống kê | 2026-01-30 |
| `total_plays` | INT | Tổng số lượt nghe trong ngày | 15,234 |
| `unique_users` | INT | Số user duy nhất đã nghe nhạc | 1,523 |
| `unique_songs` | INT | Số bài hát duy nhất được nghe | 892 |
| `unique_artists` | INT | Số nghệ sĩ duy nhất được nghe | 345 |
| `total_sessions` | INT | Tổng số phiên nghe nhạc | 2,156 |
| `paid_plays` | INT | Lượt nghe từ user trả phí | 8,123 |
| `free_plays` | INT | Lượt nghe từ user miễn phí | 7,111 |
| `paid_users` | INT | Số user trả phí active | 456 |
| `free_users` | INT | Số user miễn phí active | 1,067 |
| `paid_plays_pct` | DECIMAL | % lượt nghe từ paid users | 53.34 |
| `paid_users_pct` | DECIMAL | % users là paid | 29.94 |
| `plays_per_user` | DECIMAL | Trung bình lượt nghe/user | 10.01 |
| `plays_per_session` | DECIMAL | Trung bình lượt nghe/session | 7.07 |
| `morning_plays` | INT | Lượt nghe 6:00-12:00 | 3,234 |
| `afternoon_plays` | INT | Lượt nghe 12:00-18:00 | 4,567 |
| `evening_plays` | INT | Lượt nghe 18:00-22:00 | 5,123 |
| `night_plays` | INT | Lượt nghe 22:00-6:00 | 2,310 |
| `plays_change` | INT | Thay đổi so với ngày trước | +234 |
| `users_change` | INT | Thay đổi users so với ngày trước | +12 |
| `plays_7day_avg` | DECIMAL | Trung bình lượt nghe 7 ngày | 14,892.57 |
| `users_7day_avg` | DECIMAL | Trung bình users 7 ngày | 1,489.43 |
| `updated_at` | TIMESTAMP | Thời gian cập nhật record | 2026-01-30 10:15:00 |

---

## 2. mart_hourly_metrics

**Mô tả**: Metrics theo giờ cho time-series analysis

**Unique Key**: `event_date` + `event_hour`

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `event_date` | DATE | Ngày thống kê | 2026-01-30 |
| `event_hour` | INT | Giờ trong ngày (0-23) | 14 |
| `time_of_day` | STRING | Khoảng thời gian | morning/afternoon/evening/night |
| `hour_timestamp` | TIMESTAMP | Timestamp đầy đủ của giờ | 2026-01-30 14:00:00 |
| `total_plays` | INT | Tổng lượt nghe trong giờ | 523 |
| `unique_users` | INT | Số user duy nhất | 156 |
| `unique_songs` | INT | Số bài hát duy nhất | 234 |
| `total_sessions` | INT | Số sessions | 189 |
| `paid_plays` | INT | Lượt nghe từ paid users | 312 |
| `free_plays` | INT | Lượt nghe từ free users | 211 |
| `plays_per_user` | DECIMAL | Lượt nghe/user trong giờ | 3.35 |
| `plays_per_session` | DECIMAL | Lượt nghe/session | 2.77 |
| `paid_ratio_pct` | DECIMAL | % paid plays | 59.66 |
| `updated_at` | TIMESTAMP | Thời gian cập nhật | 2026-01-30 15:00:00 |

---

## 3. mart_active_users

**Mô tả**: User engagement và activity summary

**Primary Key**: `user_id`

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `user_id` | INT | ID của user | 12345 |
| `full_name` | STRING | Tên đầy đủ | John Doe |
| `current_level` | STRING | Subscription level hiện tại | paid/free |
| `location` | STRING | Địa điểm (BigQuery) | San Francisco, CA |
| `city` | STRING | Thành phố (PostgreSQL) | San Francisco |
| `state` | STRING | Bang (PostgreSQL) | CA |
| **Activity Metrics** |||
| `total_plays` | INT | Tổng lượt nghe của user | 1,234 |
| `unique_songs` | INT | Số bài hát khác nhau đã nghe | 456 |
| `unique_artists` | INT | Số nghệ sĩ khác nhau đã nghe | 123 |
| `total_sessions` | INT | Tổng số phiên nghe nhạc | 89 |
| `active_days` | INT | Số ngày có hoạt động | 45 |
| **Engagement Metrics** |||
| `avg_plays_per_active_day` | DECIMAL | TB lượt nghe/ngày active | 27.42 |
| `avg_plays_per_session` | DECIMAL | TB lượt nghe/session | 13.87 |
| `song_diversity_pct` | DECIMAL | % đa dạng bài hát nghe | 85.5 |
| `engagement_tier` | STRING | Mức độ engagement | high/medium/low |
| **Time Preferences** |||
| `favorite_time` | STRING | Thời điểm nghe nhiều nhất | evening |
| `preferred_days` | STRING | Các ngày nghe nhiều | weekday/weekend |
| `morning_plays` | INT | Lượt nghe buổi sáng | 123 |
| `afternoon_plays` | INT | Lượt nghe buổi chiều | 234 |
| `evening_plays` | INT | Lượt nghe buổi tối | 567 |
| `night_plays` | INT | Lượt nghe đêm | 310 |
| `weekend_plays` | INT | Lượt nghe cuối tuần | 456 |
| `weekday_plays` | INT | Lượt nghe ngày thường | 778 |
| **Timeline** |||
| `first_listen_at` | TIMESTAMP | Lần nghe đầu tiên | 2025-12-01 08:30:00 |
| `last_listen_at` | TIMESTAMP | Lần nghe gần nhất | 2026-01-30 21:45:00 |
| `listening_span_days` | INT | Số ngày từ lần đầu đến gần nhất | 60 |
| `days_since_last_listen` | INT | Số ngày kể từ lần nghe cuối | 1 |
| `is_active` | BOOLEAN | User còn active (7 ngày)? | true |
| `updated_at` | TIMESTAMP | Thời gian cập nhật | 2026-01-30 10:15:00 |

---

## 4. mart_top_songs

**Mô tả**: Top 100 bài hát theo tổng lượt nghe

**Primary Key**: `rank`

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `rank` | INT | Thứ hạng (1-100) | 1 |
| `song` | STRING | Tên bài hát | Shape of You |
| `artist` | STRING | Tên nghệ sĩ | Ed Sheeran |
| `total_plays` | INT | Tổng lượt nghe | 15,234 |
| `unique_listeners` | INT | Số người nghe duy nhất | 3,456 |
| `unique_sessions` | INT | Số sessions có bài hát này | 4,567 |
| `paid_plays` | INT | Lượt nghe từ paid users | 9,123 |
| `free_plays` | INT | Lượt nghe từ free users | 6,111 |
| `paid_ratio_pct` | DECIMAL | % paid plays | 59.89 |
| `first_played_at` | TIMESTAMP | Lần đầu được nghe | 2025-11-15 10:30:00 |
| `last_played_at` | TIMESTAMP | Lần gần nhất được nghe | 2026-01-30 22:15:00 |
| `days_with_plays` | INT | Số ngày có lượt nghe | 76 |
| `avg_plays_per_day` | DECIMAL | TB lượt nghe/ngày | 200.45 |
| `peak_time_of_day` | STRING | Thời điểm nghe nhiều nhất | evening |
| `plays_per_listener` | DECIMAL | TB lượt nghe/người nghe | 4.41 |
| `updated_at` | TIMESTAMP | Thời gian cập nhật | 2026-01-30 10:15:00 |

---

## 5. mart_top_artists

**Mô tả**: Top 100 nghệ sĩ theo tổng lượt nghe

**Primary Key**: `rank`

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `rank` | INT | Thứ hạng (1-100) | 1 |
| `artist` | STRING | Tên nghệ sĩ | Taylor Swift |
| `total_songs` | INT | Số bài hát của nghệ sĩ | 45 |
| `total_plays` | INT | Tổng lượt nghe tất cả bài | 89,234 |
| `total_listeners` | INT | Tổng người nghe (sum) | 12,345 |
| `paid_plays` | INT | Lượt nghe từ paid users | 52,123 |
| `free_plays` | INT | Lượt nghe từ free users | 37,111 |
| `avg_plays_per_song` | DECIMAL | TB lượt nghe/bài hát | 1,982.98 |
| `plays_per_listener` | DECIMAL | TB lượt nghe/người nghe | 7.23 |
| `paid_ratio_pct` | DECIMAL | % paid plays | 58.41 |
| `first_played_at` | TIMESTAMP | Bài đầu tiên được nghe | 2025-10-01 08:00:00 |
| `last_played_at` | TIMESTAMP | Lần gần nhất được nghe | 2026-01-30 23:30:00 |
| `updated_at` | TIMESTAMP | Thời gian cập nhật | 2026-01-30 10:15:00 |

---

## 6. mart_location_analytics

**Mô tả**: Phân tích geographic distribution

**Unique Key**: `location`

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `rank` | INT | Thứ hạng theo plays | 1 |
| `city` | STRING | Thành phố (PostgreSQL only) | San Francisco |
| `state` | STRING | Bang (PostgreSQL only) | CA |
| `location` | STRING | City, State combined | San Francisco, CA |
| `total_plays` | INT | Tổng lượt nghe từ location | 25,678 |
| `unique_users` | INT | Số users từ location | 2,345 |
| `unique_songs` | INT | Bài hát được nghe | 1,234 |
| `unique_artists` | INT | Nghệ sĩ được nghe | 567 |
| `paid_users` | INT | Số paid users | 789 |
| `free_users` | INT | Số free users | 1,556 |
| `plays_per_user` | DECIMAL | TB lượt nghe/user | 10.95 |
| `paid_user_pct` | DECIMAL | % paid users | 33.65 |
| `first_activity` | TIMESTAMP | Hoạt động đầu tiên | 2025-10-15 09:00:00 |
| `last_activity` | TIMESTAMP | Hoạt động gần nhất | 2026-01-30 23:45:00 |
| `updated_at` | TIMESTAMP | Thời gian cập nhật | 2026-01-30 10:15:00 |

---

## 📈 Looker Studio Field Types

Khi tạo Data Source trong Looker Studio, cấu hình các field như sau:

### Date & Time Fields
| Field | Looker Type |
|-------|-------------|
| `event_date` | Date |
| `hour_timestamp` | Date Hour |
| `first_listen_at`, `last_listen_at` | Date Hour |
| `updated_at` | Date Hour (ẩn) |

### Dimension Fields
| Field | Looker Type |
|-------|-------------|
| `user_id`, `rank` | Number |
| `full_name`, `song`, `artist`, `location` | Text |
| `current_level`, `time_of_day`, `engagement_tier` | Text |
| `is_active` | Boolean |

### Metric Fields
| Field | Looker Type | Aggregation |
|-------|-------------|-------------|
| `total_plays`, `unique_users`, `unique_songs` | Number | SUM |
| `paid_plays`, `free_plays` | Number | SUM |
| `plays_per_user`, `paid_ratio_pct` | Number | AVG |

---

## 🔗 Relationships Between Tables

```
mart_daily_summary ─────┐
                        │
mart_hourly_metrics ────┼──── event_date (time dimension)
                        │
mart_location_analytics─┘

mart_active_users ──── user engagement focus

mart_top_songs ────┬──── content performance
                   │
mart_top_artists ──┘
```

---

## 📝 Notes

1. **BigQuery vs PostgreSQL**: Một số cột có sự khác biệt:
   - BigQuery: `location` (combined)
   - PostgreSQL: `city`, `state`, `location`

2. **Incremental Tables**: `mart_daily_summary`, `mart_hourly_metrics`, `mart_location_analytics` được cập nhật incremental để tối ưu performance.

3. **Timezone**: Tất cả timestamps sử dụng UTC.

4. **Update Frequency**: 
   - Hourly metrics: Mỗi giờ
   - Daily summary: Mỗi ngày
   - Other tables: Theo schedule của Dagster pipeline
