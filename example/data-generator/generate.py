#!/usr/bin/env python3
"""
Data generator for beacon blocks - simulates multiple clients observing blocks
"""
import random
import time
from datetime import datetime, timedelta
import clickhouse_connect
import os
import sys

# Configuration from environment
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
INTERVAL_SECONDS = int(os.getenv('INTERVAL_SECONDS', '12'))  # One block every 12 seconds
BACKFILL_HOURS = int(os.getenv('BACKFILL_HOURS', '2'))  # Hours of historical data to generate

def generate_block_root():
    """Generate a random block root"""
    return f"0x{''.join(random.choices('0123456789abcdef', k=64))}"

def initialize_validator_entities():
    """Initialize validator to entity mappings (indexes 1-50)"""
    entities = [
        "coinbase", "lido", "rocketpool", "kraken", "binance",
        "homestaker", "bitstamp", "figment", "p2p", "everstake",
        "stakefish", "chorus_one", "kiln", "blockdaemon", "stakewise"
    ]
    
    # Create initial mappings - some entities own multiple validators
    validator_entities = {}
    for i in range(1, 51):
        # Weighted selection - larger entities get more validators
        if i <= 10:
            entity = entities[0]  # coinbase gets 1-10
        elif i <= 18:
            entity = entities[1]  # lido gets 11-18  
        elif i <= 24:
            entity = entities[2]  # rocketpool gets 19-24
        elif i <= 28:
            entity = entities[3]  # kraken gets 25-28
        elif i <= 31:
            entity = entities[4]  # binance gets 29-31
        else:
            # Rest distributed among smaller operators and homestakers
            entity = random.choice(entities[5:])
        validator_entities[i] = entity
    
    return validator_entities

def maybe_change_validator_entity(validator_entities, slot):
    """Occasionally change a validator's entity (simulating validator churn)"""
    entities = [
        "coinbase", "lido", "rocketpool", "kraken", "binance",
        "homestaker", "bitstamp", "figment", "p2p", "everstake",
        "stakefish", "chorus_one", "kiln", "blockdaemon", "stakewise"
    ]
    
    # 2% chance per slot to change one validator's entity
    if random.random() < 0.02:
        validator_to_change = random.randint(1, 50)
        old_entity = validator_entities[validator_to_change]
        new_entity = random.choice([e for e in entities if e != old_entity])
        validator_entities[validator_to_change] = new_entity
        return validator_to_change, old_entity, new_entity
    return None

def generate_block_observations(slot_time, validator_index, block_root=None, start_client_id=1, is_backfill=False):
    """Generate multiple client observations for a single block"""
    slot = int(slot_time.timestamp()) // 12
    if block_root is None:
        block_root = generate_block_root()
    
    # Random number of clients (1-5) that observed this block
    num_clients = random.randint(1, 5)
    
    observations = []
    for i in range(num_clients):
        # Propagation time: varies based on whether it's backfill
        if is_backfill:
            # Historical data has more varied propagation times
            propagation_ms = random.randint(500, 60000)
        else:
            # Real-time data has tighter propagation
            propagation_ms = random.randint(0, 4000)
        
        observation = {
            'updated_date_time': datetime.utcnow(),
            'event_date_time': datetime.utcnow(),
            'slot_start_date_time': slot_time,
            'slot': slot,
            'block_root': block_root,
            'validator_index': validator_index,
            'propagation_slot_start_diff': propagation_ms,
            'meta_client_name': f'client{start_client_id + i}'
        }
        observations.append(observation)
    
    return observations, num_clients

def generate_validator_entity_records(slot_time, validator_entities):
    """Generate validator entity mappings for a slot"""
    slot = int(slot_time.timestamp()) // 12
    
    records = []
    for validator_index, entity_name in validator_entities.items():
        record = {
            'updated_date_time': datetime.utcnow(),
            'event_date_time': datetime.utcnow(),
            'slot_start_date_time': slot_time,
            'slot': slot,
            'validator_index': validator_index,
            'entity_name': entity_name
        }
        records.append(record)
    
    return records

def generate_raw_transactions(slot_time, count=5):
    """Generate raw transaction records for a time period"""
    records = []
    base_position = int(slot_time.timestamp())
    currencies = ['ETH', 'USDC', 'DAI', 'WBTC', 'USDT', 'EUR', 'GBP', 'JPY']
    
    for i in range(count):
        transaction_id = f"tx_{base_position}_{i}_{random.randint(1000, 9999)}"
        records.append({
            'transaction_id': transaction_id,
            'timestamp': slot_time + timedelta(seconds=random.randint(0, 11)),
            'amount': round(random.uniform(0.001, 1000.0), 8),
            'currency': random.choice(currencies),
            'position': base_position + i
        })
    
    return records

def backfill_historical_data(client, validator_entities, hours_back=2):
    """Generate and insert historical data for the past N hours"""
    print(f"\n=== Starting backfill for {hours_back} hours of historical data ===")
    
    # Calculate time range
    current_time = datetime.utcnow()
    start_time = current_time - timedelta(hours=hours_back)
    
    # Align to 12-second slot boundary
    seconds_since_epoch = int(start_time.timestamp())
    aligned_start = (seconds_since_epoch // 12) * 12
    slot_time = datetime.fromtimestamp(aligned_start)
    
    slots_to_generate = (hours_back * 3600) // 12
    slots_generated = 0
    blocks_generated = 0
    entities_generated = 0
    
    print(f"Generating data from {slot_time} to {current_time}")
    print(f"Approximately {slots_to_generate} slots to generate...")
    
    # Batch inserts for better performance
    beacon_blocks_batch = []
    validator_entity_batch = []
    raw_transactions_batch = []
    batch_size = 100
    
    while slot_time < current_time:
        slot = int(slot_time.timestamp()) // 12
        
        # Randomly skip some slots to create gaps (20% chance)
        skip_beacon_block = random.random() < 0.20
        skip_validator_entity = random.random() < 0.15
        
        # Generate beacon block observations
        if not skip_beacon_block:
            validator_index = random.randint(1, 50)
            observations, _ = generate_block_observations(slot_time, validator_index, is_backfill=True)
            
            for obs in observations:
                beacon_blocks_batch.append([obs[col] for col in obs.keys()])
                if len(beacon_blocks_batch) >= batch_size:
                    # Insert batch
                    client.insert(
                        'ethereum.beacon_blocks',
                        beacon_blocks_batch,
                        column_names=list(observations[0].keys())
                    )
                    blocks_generated += len(beacon_blocks_batch)
                    beacon_blocks_batch = []
        
        # Generate validator entity records
        if not skip_validator_entity:
            entity_records = generate_validator_entity_records(slot_time, validator_entities)
            
            for rec in entity_records:
                validator_entity_batch.append([rec[col] for col in rec.keys()])
                if len(validator_entity_batch) >= batch_size * 50:  # Larger batch for entity records
                    # Insert batch
                    client.insert(
                        'ethereum.validator_entity',
                        validator_entity_batch,
                        column_names=list(entity_records[0].keys())
                    )
                    entities_generated += len(validator_entity_batch)
                    validator_entity_batch = []
        
        # Generate raw transactions (90% chance to generate)
        if random.random() > 0.10:
            raw_tx_records = generate_raw_transactions(slot_time, count=random.randint(3, 10))
            for rec in raw_tx_records:
                raw_transactions_batch.append([rec[col] for col in rec.keys()])
                if len(raw_transactions_batch) >= batch_size:
                    # Insert batch
                    client.insert(
                        'ethereum.raw_transactions',
                        raw_transactions_batch,
                        column_names=list(raw_tx_records[0].keys())
                    )
                    raw_transactions_batch = []
        
        # Occasionally change validator entities during backfill
        maybe_change_validator_entity(validator_entities, slot_time)
        
        slots_generated += 1
        if slots_generated % 100 == 0:
            print(f"  Progress: {slots_generated}/{slots_to_generate} slots ({(slots_generated/slots_to_generate)*100:.1f}%)")
        
        # Move to next slot
        slot_time = slot_time + timedelta(seconds=12)
    
    # Insert remaining batches
    if beacon_blocks_batch:
        client.insert(
            'ethereum.beacon_blocks',
            beacon_blocks_batch,
            column_names=['updated_date_time', 'event_date_time', 'slot_start_date_time', 
                         'slot', 'block_root', 'validator_index', 'propagation_slot_start_diff', 
                         'meta_client_name']
        )
        blocks_generated += len(beacon_blocks_batch)
    
    if validator_entity_batch:
        client.insert(
            'ethereum.validator_entity',
            validator_entity_batch,
            column_names=['updated_date_time', 'event_date_time', 'slot_start_date_time',
                         'slot', 'validator_index', 'entity_name']
        )
        entities_generated += len(validator_entity_batch)
    
    if raw_transactions_batch:
        client.insert(
            'ethereum.raw_transactions',
            raw_transactions_batch,
            column_names=['transaction_id', 'timestamp', 'amount', 'currency', 'position']
        )
    
    print(f"\n=== Backfill complete ===")
    print(f"  Slots processed: {slots_generated}")
    print(f"  Block observations inserted: {blocks_generated}")
    print(f"  Entity records inserted: {entities_generated}")
    print()

def main():
    print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    
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
    
    # Ensure database and table exist
    try:
        client.command("CREATE DATABASE IF NOT EXISTS ethereum")
        print("Database 'ethereum' ready")
    except Exception as e:
        print(f"Warning creating database: {e}")
    
    # Initialize validator entities
    validator_entities = initialize_validator_entities()
    print(f"Initialized {len(validator_entities)} validator entity mappings")
    
    # Perform initial backfill if configured
    if BACKFILL_HOURS > 0:
        backfill_historical_data(client, validator_entities, BACKFILL_HOURS)
    
    print(f"\n=== Starting real-time block generation (interval={INTERVAL_SECONDS}s) ===")
    print("1:1 ratio - for each forward fill, also backfilling earliest slot")
    print("Will also insert late observations for recent blocks...")
    
    # Start from current time, aligned to 12-second slots
    current_time = datetime.utcnow()
    # Align to next 12-second boundary
    seconds_since_epoch = int(current_time.timestamp())
    next_slot_time = ((seconds_since_epoch // 12) + 1) * 12
    slot_time = datetime.fromtimestamp(next_slot_time)
    
    iteration = 0
    previous_slot_info = None  # Store info about previous slot for late inserts
    slot_history = []  # Keep history of recent slots for delayed inserts
    
    while True:
        try:
            # Get the current minimum slot to know where to backfill
            min_result = client.query("SELECT min(slot_start_date_time) as min_time FROM ethereum.beacon_blocks")
            min_time_str = min_result.result_rows[0][0] if min_result.result_rows and min_result.result_rows[0][0] else None
            
            if min_time_str:
                # min_time_str is already a datetime object from ClickHouse
                min_time = min_time_str
                backfill_slot_time = min_time - timedelta(seconds=12)
                backfill_slot = int(backfill_slot_time.timestamp()) // 12
                
                # Generate backfill data (20% skip rate like initial backfill)
                if random.random() > 0.20:
                    backfill_validator_index = random.randint(1, 50)
                    backfill_observations, _ = generate_block_observations(backfill_slot_time, backfill_validator_index, is_backfill=True)
                    
                    # Insert backfill beacon blocks
                    column_names = list(backfill_observations[0].keys())
                    data = [[obs[col] for col in column_names] for obs in backfill_observations]
                    client.insert('ethereum.beacon_blocks', data, column_names=column_names)
                    
                    # Also generate validator entities (15% skip rate)
                    if random.random() > 0.15:
                        backfill_entity_records = generate_validator_entity_records(backfill_slot_time, validator_entities)
                        entity_column_names = list(backfill_entity_records[0].keys())
                        entity_data = [[rec[col] for col in entity_column_names] for rec in backfill_entity_records]
                        client.insert('ethereum.validator_entity', entity_data, column_names=entity_column_names)
                    
                    # Generate raw transactions for backfill (95% chance)
                    if random.random() > 0.05:
                        backfill_tx_records = generate_raw_transactions(backfill_slot_time, count=random.randint(3, 10))
                        if backfill_tx_records:
                            tx_column_names = list(backfill_tx_records[0].keys())
                            tx_data = [[rec[col] for col in tx_column_names] for rec in backfill_tx_records]
                            client.insert('ethereum.raw_transactions', tx_data, column_names=tx_column_names)
                    
                    print(f"  🔙 BACKFILL: Slot {backfill_slot} (val:{backfill_validator_index}/{validator_entities[backfill_validator_index]})")
            
            # Independent skipping for beacon_blocks, validator_entity, and transactions
            skip_beacon_block = random.random() < 0.15  # 15% chance to skip beacon block
            skip_validator_entity = random.random() < 0.10  # 10% chance to skip validator entity
            skip_transactions = random.random() < 0.05  # 5% chance to skip transactions
            
            slot = int(slot_time.timestamp()) // 12
            
            # Always check for entity changes (internal state tracking)
            entity_change = maybe_change_validator_entity(validator_entities, slot_time)
            if entity_change:
                val_idx, old_entity, new_entity = entity_change
                print(f"  → Validator {val_idx} changed from {old_entity} to {new_entity}")
            
            # Handle beacon_blocks generation
            observations = None
            validator_index = None
            if skip_beacon_block:
                print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Slot {slot}: SKIPPED beacon_block")
            else:
                # Randomly select a validator for this slot (1-50)
                validator_index = random.randint(1, 50)
                # Generate observations for current slot
                observations, num_clients = generate_block_observations(slot_time, validator_index)
            
            # Handle validator_entity generation
            entity_records = None
            if skip_validator_entity:
                if not skip_beacon_block:  # Only log if we're not already logging a skip
                    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Slot {slot}: SKIPPED validator_entity")
            else:
                # Generate validator entity records for this slot
                entity_records = generate_validator_entity_records(slot_time, validator_entities)
            
            if observations:
                # Prepare data for insertion
                column_names = list(observations[0].keys())
                data = [[obs[col] for col in column_names] for obs in observations]
                
                # Insert into ClickHouse
                client.insert(
                    'ethereum.beacon_blocks',
                    data,
                    column_names=column_names
                )
                
                iteration += 1
                block_root = observations[0]['block_root']
                block_root_short = block_root[:10] + '...'
                entity = validator_entities[validator_index]
                print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Block {iteration}: "
                      f"Slot {slot} (val:{validator_index}/{entity}, {len(observations)} clients saw {block_root_short})")
            
            # Insert validator entity mappings separately (independent of beacon_blocks)
            if entity_records:
                entity_column_names = list(entity_records[0].keys())
                entity_data = [[rec[col] for col in entity_column_names] for rec in entity_records]
                client.insert(
                    'ethereum.validator_entity',
                    entity_data,
                    column_names=entity_column_names
                )
                if skip_beacon_block:  # Only log entity insertion if beacon_block was skipped
                    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Slot {slot}: validator_entity records inserted")
            
            # Generate and insert raw transactions
            if not skip_transactions:
                transaction_count = random.randint(2, 15)  # 2-15 transactions per slot
                transaction_records = generate_raw_transactions(slot_time, transaction_count)
                if transaction_records:
                    transaction_column_names = list(transaction_records[0].keys())
                    transaction_data = [[rec[col] for col in transaction_column_names] for rec in transaction_records]
                    
                    client.insert(
                        'ethereum.raw_transactions',
                        transaction_data,
                        column_names=transaction_column_names
                    )
                    # Log if verbose output desired
                    # print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Slot {slot}: {len(transaction_data)} raw transactions inserted")
                
            # Add to slot history only if we generated beacon_blocks
            if observations:
                slot_history.append({
                    'slot_time': slot_time,
                    'slot': slot,
                    'block_root': observations[0]['block_root'],
                    'validator_index': validator_index,
                    'num_clients': len(observations),
                    'added_at': datetime.utcnow()
                })
                
                # Keep only last 10 slots (120 seconds worth)
                if len(slot_history) > 10:
                    slot_history.pop(0)
            
            # Process delayed inserts for historical slots (only if we have history)
            if slot_history:
                for hist_slot in slot_history[:-1] if observations else slot_history:  # Skip current slot if we just added it
                    age_seconds = (datetime.utcnow() - hist_slot['added_at']).total_seconds()
                    
                    # Different probabilities based on age
                    if age_seconds < 30:
                        continue  # Too recent, skip
                    elif age_seconds < 60:
                        insert_prob = 0.1  # 10% chance for 30-60s old
                    elif age_seconds < 90:
                        insert_prob = 0.2  # 20% chance for 60-90s old
                    else:
                        insert_prob = 0.3  # 30% chance for 90-120s old
                    
                    # Check if we should add a delayed observation
                    if hist_slot['num_clients'] < 5 and random.random() < insert_prob:
                        late_client_id = hist_slot['num_clients'] + 1
                        propagation_delay = int(age_seconds * 1000) + random.randint(0, 5000)
                        
                        late_observation = {
                            'updated_date_time': datetime.utcnow(),
                            'event_date_time': datetime.utcnow(),
                            'slot_start_date_time': hist_slot['slot_time'],
                            'slot': hist_slot['slot'],
                            'block_root': hist_slot['block_root'],
                            'validator_index': hist_slot['validator_index'],
                            'propagation_slot_start_diff': propagation_delay,
                            'meta_client_name': f'client{late_client_id}'
                        }
                        
                        # Insert late observation
                        client.insert(
                            'ethereum.beacon_blocks',
                            [[late_observation[col] for col in late_observation.keys()]],
                            column_names=list(late_observation.keys())
                        )
                        
                        print(f"  └─ DELAYED ({int(age_seconds)}s): {late_observation['meta_client_name']} "
                              f"reported slot {hist_slot['slot']} (prop: {propagation_delay}ms)")
                        
                        # Update the client count to prevent duplicate inserts
                        hist_slot['num_clients'] += 1
            
            # Move to next slot (12 seconds later)
            slot_time = slot_time + timedelta(seconds=12)
            
            # Wait until it's time for the next block
            sleep_time = (slot_time - datetime.utcnow()).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            print("\nShutting down data generator...")
            break
        except Exception as e:
            print(f"Error generating data: {e}")
            # Continue to next slot even on error
            slot_time = slot_time + timedelta(seconds=12)
            time.sleep(INTERVAL_SECONDS)
    
    client.close()
    print("Data generator stopped")

if __name__ == '__main__':
    main()