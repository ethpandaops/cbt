#!/usr/bin/env python3
"""
Entity Changes Transformation Model
Analyzes validator entity changes over time using Python

This demonstrates how to write a Python-based transformation model that:
1. Receives ClickHouse credentials and context via environment variables
2. Queries dependency data via HTTP interface
3. Performs transformations in Python
4. Writes results back to ClickHouse
"""

import os
import sys
from urllib.parse import urlparse
from datetime import datetime
import urllib.request
import urllib.parse
import json

def execute_clickhouse_query(host, query):
    """Execute a query via ClickHouse HTTP interface"""
    # Always use port 8123 for HTTP interface
    url = f"http://{host}:8123/"
    
    # For SELECT queries, request JSON format
    if 'SELECT' in query.upper():
        url += '?default_format=JSONCompact'
    
    # Make POST request with query in body
    try:
        req = urllib.request.Request(url, 
                                    data=query.encode('utf-8'),
                                    method='POST')
        response = urllib.request.urlopen(req)
        result = response.read().decode('utf-8')
        
        if result.strip():
            # Try to parse as JSON for SELECT queries
            if 'SELECT' in query.upper():
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    return {'data': []}
        return {'data': []}
    except Exception as e:
        print(f"Query failed: {e}", file=sys.stderr)
        print(f"URL: {url}", file=sys.stderr)
        print(f"Query: {query[:200]}...", file=sys.stderr)
        raise

def main():
    # Get environment variables provided by CBT worker
    ch_url = os.environ['CLICKHOUSE_URL']
    
    # Parse to get hostname (ignore port since we'll use 8123 for HTTP)
    parsed = urlparse(ch_url)
    ch_host = parsed.hostname or 'clickhouse'
    
    # Task context
    bounds_start = int(os.environ['BOUNDS_START'])
    bounds_end = int(os.environ['BOUNDS_END'])
    task_start = int(os.environ['TASK_START'])
    
    # Model info
    target_db = os.environ['SELF_DATABASE']
    target_table = os.environ['SELF_TABLE']
    
    # Dependency info
    dep_db = os.environ.get('DEP_ETHEREUM_VALIDATOR_ENTITY_DATABASE', 'ethereum')
    dep_table = os.environ.get('DEP_ETHEREUM_VALIDATOR_ENTITY_TABLE', 'validator_entity')
    
    print(f"=== Python Transformation Model Execution ===")
    print(f"Time range: {datetime.fromtimestamp(bounds_start)} to {datetime.fromtimestamp(bounds_end)}")
    print(f"Source: {dep_db}.{dep_table}")
    print(f"Target: {target_db}.{target_table}")
    print(f"ClickHouse host: {ch_host}")
    
    try:
        # Create target table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_db}.{target_table} (
            updated_date_time DateTime,
            event_date_time DateTime,
            hour_start DateTime,
            entity_name String,
            validator_count UInt32,
            unique_slots UInt32,
            position UInt64
        ) ENGINE = ReplacingMergeTree(updated_date_time)
        PARTITION BY toYYYYMM(hour_start)
        ORDER BY (hour_start, entity_name, position)
        """
        execute_clickhouse_query(ch_host, create_table_query)
        print("✓ Ensured target table exists")
        
        # Simple aggregation: Count validators per entity in this time window
        analysis_query = f"""
        SELECT 
            entity_name,
            COUNT(DISTINCT validator_index) as validator_count,
            COUNT(DISTINCT slot) as unique_slots
        FROM {dep_db}.{dep_table}
        WHERE slot_start_date_time >= fromUnixTimestamp({bounds_start})
          AND slot_start_date_time < fromUnixTimestamp({bounds_end})
          AND entity_name != ''
        GROUP BY entity_name
        ORDER BY validator_count DESC
        FORMAT JSONCompact
        """
        
        result = execute_clickhouse_query(ch_host, analysis_query)
        
        if 'data' in result and result['data']:
            entities = result['data']
            print(f"✓ Found {len(entities)} entities with activity")
            
            # Process results in Python (example: calculate additional metrics)
            # JSONCompact returns strings, convert to int
            total_validators = sum(int(row[1]) for row in entities)
            avg_validators = total_validators / len(entities) if entities else 0
            
            print(f"  Total validators: {total_validators}")
            print(f"  Average per entity: {avg_validators:.1f}")
            
            # Insert processed results back to ClickHouse
            for row in entities:
                entity_name = row[0]
                validator_count = int(row[1])
                unique_slots = int(row[2])
                # Escape single quotes in entity name
                entity_name_escaped = entity_name.replace("'", "\\'")
                insert_query = f"""
                INSERT INTO {target_db}.{target_table} 
                (updated_date_time, event_date_time, hour_start, 
                 entity_name, validator_count, unique_slots, position)
                VALUES
                ({task_start}, now(), fromUnixTimestamp({bounds_start}),
                 '{entity_name_escaped}', {validator_count}, {unique_slots}, {bounds_start})
                """
                execute_clickhouse_query(ch_host, insert_query)
            
            print(f"✓ Inserted {len(entities)} entity statistics")
            
            # Demonstrate Python-specific processing capabilities
            if entities:
                high_activity = [e for e in entities if int(e[1]) > avg_validators * 2]
                if high_activity:
                    print(f"\n⚠ High activity entities (>{avg_validators*2:.0f} validators):")
                    for entity in high_activity[:3]:  # Show top 3
                        print(f"  - {entity[0]}: {int(entity[1])} validators")
        else:
            print("No entity data found in this time range")
        
        print("\n✅ Python transformation completed successfully")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error processing entity changes: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())