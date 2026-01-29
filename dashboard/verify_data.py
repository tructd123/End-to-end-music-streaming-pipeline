"""
Dashboard Data Verification
============================
Verify BigQuery data is ready for dashboard
"""

import os
from pathlib import Path

def verify_dashboard_data():
    """Verify all mart tables have data for dashboard"""
    from google.cloud import bigquery
    
    # Set credentials
    credentials_path = Path(__file__).parent.parent / "credentials" / "dbt-sa-key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)
    
    project_id = "graphic-boulder-483814-g7"
    client = bigquery.Client(project=project_id)
    
    # Tables to check
    tables = [
        ("staging_marts.mart_top_songs", "Top Songs"),
        ("staging_marts.mart_top_artists", "Top Artists"),
        ("staging_marts.mart_active_users", "Active Users"),
        ("staging_marts.mart_daily_summary", "Daily Summary"),
        ("staging_marts.mart_hourly_metrics", "Hourly Metrics"),
        ("staging_marts.mart_location_analytics", "Location Analytics"),
    ]
    
    print("=" * 60)
    print("SoundFlow Dashboard Data Verification")
    print("=" * 60)
    print(f"Project: {project_id}")
    print()
    
    all_ready = True
    total_records = 0
    
    for table_name, display_name in tables:
        try:
            query = f"SELECT COUNT(*) as cnt FROM `{project_id}.{table_name}`"
            result = list(client.query(query).result())[0]
            count = result.cnt
            total_records += count
            
            status = "[OK]" if count > 0 else "[EMPTY]"
            print(f"  {status} {display_name}: {count:,} rows")
            
            if count == 0:
                all_ready = False
                
        except Exception as e:
            print(f"  [ERROR] {display_name}: {e}")
            all_ready = False
    
    print()
    print("-" * 60)
    print(f"Total Records: {total_records:,}")
    print()
    
    if all_ready:
        print("[OK] All tables ready for dashboard!")
        print()
        print("Next steps:")
        print("  1. Open Looker Studio: https://lookerstudio.google.com/")
        print("  2. Create New Report")
        print("  3. Add BigQuery data source")
        print("  4. Select project: graphic-boulder-483814-g7")
        print("  5. Select dataset: staging_marts")
    else:
        print("[WARNING] Some tables are empty or missing!")
        print("Run dbt pipeline first:")
        print("  cd dbt && dbt run --target prod")
    
    print("=" * 60)
    
    return all_ready


def show_sample_data():
    """Show sample data from each table"""
    from google.cloud import bigquery
    
    credentials_path = Path(__file__).parent.parent / "credentials" / "dbt-sa-key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)
    
    project_id = "graphic-boulder-483814-g7"
    client = bigquery.Client(project=project_id)
    
    print("\n" + "=" * 60)
    print("Sample Data Preview")
    print("=" * 60)
    
    # Top 5 Songs
    print("\n--- Top 5 Songs ---")
    query = f"""
        SELECT rank, song, artist, total_plays, unique_listeners
        FROM `{project_id}.staging_marts.mart_top_songs`
        WHERE rank <= 5
        ORDER BY rank
    """
    for row in client.query(query).result():
        print(f"  #{row.rank}: {row.song} by {row.artist} ({row.total_plays:,} plays)")
    
    # Today's KPIs
    print("\n--- Today's KPIs ---")
    query = f"""
        SELECT event_date, total_plays, unique_users, paid_plays_pct
        FROM `{project_id}.staging_marts.mart_daily_summary`
        ORDER BY event_date DESC
        LIMIT 1
    """
    for row in client.query(query).result():
        print(f"  Date: {row.event_date}")
        print(f"  Total Plays: {row.total_plays:,}")
        print(f"  Unique Users: {row.unique_users:,}")
        print(f"  Paid %: {row.paid_plays_pct:.1f}%")
    
    # Engagement Distribution
    print("\n--- User Engagement Tiers ---")
    query = f"""
        SELECT engagement_tier, COUNT(*) as cnt
        FROM `{project_id}.staging_marts.mart_active_users`
        GROUP BY engagement_tier
        ORDER BY cnt DESC
    """
    for row in client.query(query).result():
        print(f"  {row.engagement_tier}: {row.cnt:,} users")


if __name__ == "__main__":
    import sys
    
    try:
        ready = verify_dashboard_data()
        
        if ready and len(sys.argv) > 1 and sys.argv[1] == "--sample":
            show_sample_data()
            
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have:")
        print("  1. google-cloud-bigquery installed: pip install google-cloud-bigquery")
        print("  2. Valid credentials at credentials/dbt-sa-key.json")
        print("  3. dbt pipeline has been run")
