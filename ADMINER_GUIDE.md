# 📘 Adminer User Guide - SoundFlow

## 🚀 Step 1: Access Adminer

1. Open your web browser
2. Go to: **http://localhost:8081**
3. You will see the login page

## 🔐 Step 2: Login

Enter the following information:

```
System:   PostgreSQL (dropdown)
Server:   postgres
Username: soundflow
Password: soundflow123
Database: soundflow
```

Then click the **"Login"** button

## 📊 Step 3: Select Schema "raw"

**IMPORTANT**: After logging in, you will see the main interface.

### Find the Schema dropdown:
- On the **left side** of the screen
- Look for **"Schema:"** or a dropdown showing **"public"**
- **Click on the dropdown** and select **"raw"**

### If you don't see the dropdown:
- In the left menu, find **"DB schema:"**
- Or look at the URL: `...&ns=public` → click on "public" and select "raw"

## 📁 Step 4: View Table List

After selecting schema "raw", you will see 5 tables:

- ✅ **listen_events** (173K+ records)
- ✅ **status_change_events** (160+ records)
- ✅ **page_view_events** (0 records)
- ✅ **auth_events** (0 records)
- ✅ **pipeline_metadata** (metadata)

## 🔍 Step 5: View Data

### Method 1: Click on table name
1. Click on **"listen_events"**
2. You will see the table structure (columns, types, indexes)
3. Click the **"Select data"** tab at the top
4. Choose how many rows to view (10, 50, 100...)
5. Click **"Select"**

### Method 2: Run SQL Query (RECOMMENDED)
1. Click **"SQL command"** in the left menu
2. Or go to URL: **http://localhost:8081/?pgsql=postgres&username=soundflow&db=soundflow&sql=**
3. Enter your SQL query
4. Click **"Execute"** or press **Ctrl+Enter**

## 💡 Basic Sample Queries

### Query 1: View first 10 records
```sql
SELECT * FROM raw.listen_events LIMIT 10;
```

### Query 2: Count total records
```sql
SELECT COUNT(*) FROM raw.listen_events;
```

### Query 3: View latest songs
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

### Query 4: Top 5 popular songs
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

## 🎯 Advanced Queries

Open the **adminer_queries.sql** file in the project to see 10 more detailed analytical queries:

1. Top popular songs
2. Top users listening the most
3. Distribution by city
4. Free vs paid user analysis
5. Activity by hour
6. And more...

## 🛠️ Other Features

### Export data:
1. Select a table
2. Click **"Export"**
3. Choose format: CSV, SQL, JSON...
4. Click **"Export"**

### Import data:
1. Click **"Import"**
2. Select file
3. Click **"Execute"**

### Create new table:
1. Click **"Create table"**
2. Enter name and column definitions
3. Click **"Save"**

## 🔧 Troubleshooting

### Don't see any tables?
✅ Check if you've selected the **"raw"** schema
✅ Try refreshing the page (F5)
✅ Logout and login again

### Query error?
✅ Make sure to include **"raw."** before table name: `raw.listen_events`
✅ Check SQL syntax
✅ Look at the error message below the query box

### Cannot connect?
✅ Check containers: `docker ps`
✅ Check if postgres is running: `docker logs postgres`
✅ Try restarting adminer: `docker restart adminer`

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

**Note**: Remember to always select schema **"raw"** after login to see the tables!
