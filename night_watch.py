
import time
import psycopg2
import subprocess
import sys
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def get_count():
    try:
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM waste_listings")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

def night_watch():
    print("🦉 NIGHT WATCH ACTIVE: Monitoring Database...")
    
    last_count = 0
    stable_checks = 0
    REQUIRED_STABILITY = 5  # Minutes of no change
    TARGET_COUNT = 850000   # Expected EU+US count
    
    while True:
        current_count = get_count()
        print(f"   Current Records: {current_count:,}")
        
        # Check for stability (End of Ingestion)
        if current_count == last_count and current_count > 500000:
            stable_checks += 1
            print(f"   Stability Count: {stable_checks}/{REQUIRED_STABILITY}")
        else:
            stable_checks = 0
            
        # Trigger Condition: 
        # 1. We hit the target size OR
        # 2. The count hasn't changed for 5 minutes (Ingestion finished)
        if current_count > TARGET_COUNT or stable_checks >= REQUIRED_STABILITY:
            print("\n🚀 INGESTION COMPLETE. TRIGGERING TRAINING DATA EXPORT...")
            
            # Run the export
            try:
                subprocess.run([sys.executable, "prepare_training_data.py"], check=True)
                print("\n✅ MISSION ACCOMPLISHED. GO TO SLEEP.")
            except Exception as e:
                print(f"❌ EXPORT FAILED: {e}")
            
            break
            
        last_count = current_count
        time.sleep(60) # Check every minute

if __name__ == "__main__":
    night_watch()
