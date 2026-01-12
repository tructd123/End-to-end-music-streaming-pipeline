# 📘 Hướng dẫn sử dụng Adminer - SoundFlow

## 🚀 Bước 1: Truy cập Adminer

1. Mở trình duyệt web
2. Truy cập: **http://localhost:8081**
3. Bạn sẽ thấy trang đăng nhập

## 🔐 Bước 2: Đăng nhập

Điền thông tin sau:

```
System:   PostgreSQL (dropdown)
Server:   postgres
Username: soundflow
Password: soundflow123
Database: soundflow
```

Sau đó click nút **"Login"**

## 📊 Bước 3: Chọn Schema "raw"

**QUAN TRỌNG**: Sau khi đăng nhập, bạn sẽ thấy giao diện chính.

### Tìm dropdown Schema:
- Ở phía **bên trái** màn hình
- Có chữ **"Schema:"** hoặc dropdown hiện **"public"**
- **Click vào dropdown đó** và chọn **"raw"**

### Nếu không thấy dropdown:
- Ở menu bên trái, tìm phần **"DB schema:"**
- Hoặc xem URL: `...&ns=public` → click vào "public" và chọn "raw"

## 📁 Bước 4: Xem danh sách bảng

Sau khi chọn schema "raw", bạn sẽ thấy 5 bảng:

- ✅ **listen_events** (173K+ records)
- ✅ **status_change_events** (160+ records)
- ✅ **page_view_events** (0 records)
- ✅ **auth_events** (0 records)
- ✅ **pipeline_metadata** (metadata)

## 🔍 Bước 5: Xem dữ liệu

### Cách 1: Click vào tên bảng
1. Click vào **"listen_events"**
2. Bạn sẽ thấy cấu trúc bảng (columns, types, indexes)
3. Click tab **"Select data"** ở phía trên
4. Chọn số dòng muốn xem (10, 50, 100...)
5. Click **"Select"**

### Cách 2: Chạy SQL Query (KHUYÊN DÙNG)
1. Click **"SQL command"** ở menu bên trái
2. Hoặc URL: **http://localhost:8081/?pgsql=postgres&username=soundflow&db=soundflow&sql=**
3. Nhập câu query SQL
4. Click **"Execute"** hoặc nhấn **Ctrl+Enter**

## 💡 Query Mẫu Cơ Bản

### Query 1: Xem 10 bản ghi đầu tiên
```sql
SELECT * FROM raw.listen_events LIMIT 10;
```

### Query 2: Đếm tổng số bản ghi
```sql
SELECT COUNT(*) FROM raw.listen_events;
```

### Query 3: Xem bài hát mới nhất
```sql
SELECT 
    event_timestamp,
    first_name,
    last_name,
    song,
    artist,
    city,
    state
FROM raw.listen_events
ORDER BY event_timestamp DESC
LIMIT 10;
```

### Query 4: Top 5 bài hát phổ biến
```sql
SELECT 
    song,
    artist,
    COUNT(*) as plays
FROM raw.listen_events
GROUP BY song, artist
ORDER BY plays DESC
LIMIT 5;
```

## 🎯 Query Nâng Cao

Mở file **adminer_queries.sql** trong project để xem thêm 10 query phân tích chi tiết:

1. Top bài hát phổ biến
2. Top users nghe nhạc nhiều
3. Phân bố theo thành phố
4. Phân tích free vs paid users
5. Activity theo giờ
6. Và nhiều hơn nữa...

## 🛠️ Các Tính Năng Khác

### Export dữ liệu:
1. Chọn bảng
2. Click **"Export"**
3. Chọn format: CSV, SQL, JSON...
4. Click **"Export"**

### Import dữ liệu:
1. Click **"Import"**
2. Chọn file
3. Click **"Execute"**

### Tạo bảng mới:
1. Click **"Create table"**
2. Điền tên và định nghĩa columns
3. Click **"Save"**

## 🔧 Troubleshooting

### Không thấy bảng nào?
✅ Kiểm tra xem đã chọn schema **"raw"** chưa
✅ Thử refresh trang (F5)
✅ Logout và login lại

### Query bị lỗi?
✅ Đảm bảo có **"raw."** trước tên bảng: `raw.listen_events`
✅ Kiểm tra syntax SQL
✅ Xem error message ở phía dưới query box

### Không kết nối được?
✅ Kiểm tra containers: `docker ps`
✅ Kiểm tra postgres đang chạy: `docker logs postgres`
✅ Thử restart adminer: `docker restart adminer`

## 📞 Quick Commands

```bash
# Restart Adminer
docker restart adminer

# Check Adminer logs
docker logs adminer

# Check PostgreSQL
docker exec -it postgres psql -U soundflow -d soundflow -c "\dt raw.*"
```

## 🌐 Direct Links

- Adminer UI: http://localhost:8081
- Redpanda Console: http://localhost:8080
- PostgreSQL Port: localhost:5432

---

**Lưu ý**: Nhớ luôn chọn schema **"raw"** sau khi login để xem được các bảng!
