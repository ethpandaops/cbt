#!/usr/bin/env python3
"""
Chaos generator - randomly drops data from various tables to test gap detection and backfill
"""
import random
import time
import clickhouse_connect
import os
import sys
from datetime import datetime, timedelta

# Configuration from environment
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CHAOS_INTERVAL_SECONDS = int(os.getenv('CHAOS_INTERVAL_SECONDS', '30'))  # How often to cause chaos
CHAOS_PROBABILITY = float(os.getenv('CHAOS_PROBABILITY', '0.3'))  # Probability of chaos per interval

# Tables to target for chaos
EXTERNAL_TABLES = [
    ('ethereum', 'beacon_blocks'),
    ('ethereum', 'validator_entity')
]

TRANSFORMATION_TABLES = [
    ('analytics', 'block_entity'),
    ('analytics', 'block_propagation'),
    ('analytics', 'entity_network_effects')
]

ADMIN_TABLE = ('admin', 'cbt')

def get_random_positions(client, database, table, count=1):
    """Get random positions from a table"""
    try:
        # For admin table, get positions
        if database == 'admin' and table == 'cbt':
            query = f"""
                SELECT database, table, position 
                FROM {database}.{table}
                WHERE position > 0
                ORDER BY rand()
                LIMIT {count}
            """
        else:
            # For data tables, get slot-based positions
            query = f"""
                SELECT DISTINCT slot
                FROM {database}.{table}
                WHERE slot > 0
                ORDER BY rand()
                LIMIT {count}
            """
        
        result = client.query(query)
        if result.result_rows:
            return result.result_rows
        return []
    except Exception as e:
        print(f"Error getting positions from {database}.{table}: {e}")
        return []

def drop_external_data(client, database, table):
    """Drop random data from external tables"""
    positions = get_random_positions(client, database, table, random.randint(1, 3))
    
    if not positions:
        print(f"  No data to drop from {database}.{table}")
        return
    
    for position in positions:
        slot = position[0]
        try:
            # Delete all records for this slot
            query = f"""
                DELETE FROM {database}.{table}
                WHERE slot = {slot}
            """
            client.command(query)
            print(f"  💥 Dropped external data: {database}.{table} slot={slot}")
        except Exception as e:
            print(f"  Error dropping data from {database}.{table}: {e}")

def drop_transformation_data(client, database, table):
    """Drop random data from transformation tables (admin.cbt only)
    
    This simulates gaps in the tracking without removing the actual transformed data.
    The coordinator should detect these gaps and re-process them.
    """
    # Get random positions for this transformation
    query = f"""
        SELECT position, interval
        FROM admin.cbt FINAL
        WHERE database = '{database}' 
        AND table = '{table}'
        AND position > 0
        ORDER BY rand()
        LIMIT {random.randint(1, 2)}
    """
    
    try:
        result = client.query(query)
        if not result.result_rows:
            print(f"  No data to drop from {database}.{table}")
            return
        
        for row in result.result_rows:
            position = row[0]
            interval = row[1]
            
            # Delete ONLY from admin.cbt - this creates a gap that should be detected and refilled
            delete_query = f"""
                DELETE FROM admin.cbt
                WHERE database = '{database}'
                AND table = '{table}'
                AND position = {position}
            """
            client.command(delete_query)
            print(f"  💥 Dropped transformation tracking: {database}.{table} position={position} (interval={interval})")
            print(f"    └─ Gap created - coordinator should detect and reprocess this interval")
                
    except Exception as e:
        print(f"  Error dropping transformation data: {e}")

def cause_chaos(client):
    """Main chaos function - only drops transformation tracking data (admin.cbt)"""
    # Only do transformation chaos - never touch external data
    print(f"\n🎲 CHAOS EVENT: transformation chaos only (external data preserved)")
    
    # Drop data from transformation tables (via admin.cbt only)
    if TRANSFORMATION_TABLES:
        for database, table in random.sample(TRANSFORMATION_TABLES, min(len(TRANSFORMATION_TABLES), random.randint(1, 2))):
            drop_transformation_data(client, database, table)
    else:
        print("  No transformation tables found")

def check_table_status(client):
    """Check current status of all tables"""
    print("\n📊 Current Table Status:")
    
    # Check external tables
    print("  External Tables:")
    for database, table in EXTERNAL_TABLES:
        try:
            query = f"SELECT count() as cnt, min(slot) as min_slot, max(slot) as max_slot FROM {database}.{table}"
            result = client.query(query)
            if result.result_rows:
                cnt, min_slot, max_slot = result.result_rows[0]
                print(f"    {database}.{table}: {cnt} rows, slots {min_slot}-{max_slot}")
        except Exception as e:
            print(f"    {database}.{table}: Error - {e}")
    
    # Check transformation status via admin.cbt
    print("  Transformation Tables (admin.cbt):")
    try:
        query = """
            SELECT 
                database || '.' || table as model,
                count() as intervals,
                min(position) as min_pos,
                max(position + interval) as max_pos
            FROM admin.cbt FINAL
            GROUP BY database, table
            ORDER BY database, table
        """
        result = client.query(query)
        for row in result.result_rows:
            model, intervals, min_pos, max_pos = row
            if min_pos and max_pos:
                min_time = datetime.fromtimestamp(min_pos).strftime('%H:%M:%S')
                max_time = datetime.fromtimestamp(max_pos).strftime('%H:%M:%S')
                print(f"    {model}: {intervals} intervals, {min_time}-{max_time}")
            else:
                print(f"    {model}: {intervals} intervals")
    except Exception as e:
        print(f"    Error querying admin.cbt: {e}")

def main():
    print(f"🌪️  CHAOS GENERATOR STARTING")
    print(f"Configuration:")
    print(f"  ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    print(f"  Chaos Interval: {CHAOS_INTERVAL_SECONDS}s")
    print(f"  Chaos Probability: {CHAOS_PROBABILITY}")
    print()
    
    # Wait a bit for other services to generate some data
    print("Waiting 60 seconds for initial data generation...")
    time.sleep(60)
    
    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        print("Connected to ClickHouse successfully")
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")
        sys.exit(1)
    
    iteration = 0
    while True:
        try:
            iteration += 1
            
            # Check if we should cause chaos this iteration
            if random.random() < CHAOS_PROBABILITY:
                print(f"\n{'='*60}")
                print(f"Chaos Iteration {iteration} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Show status before chaos
                check_table_status(client)
                
                # Cause chaos
                cause_chaos(client)
                
                # Show status after chaos
                time.sleep(2)  # Wait for changes to propagate
                check_table_status(client)
                
                print(f"{'='*60}")
            else:
                print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Iteration {iteration}: No chaos this time (probability check)")
            
            # Wait for next chaos opportunity
            time.sleep(CHAOS_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            print("\n🛑 Chaos generator shutting down...")
            break
        except Exception as e:
            print(f"Error in chaos loop: {e}")
            time.sleep(CHAOS_INTERVAL_SECONDS)
    
    client.close()
    print("Chaos generator stopped")

if __name__ == '__main__':
    main()