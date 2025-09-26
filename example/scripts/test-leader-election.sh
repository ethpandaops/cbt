#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== CBT Leader Election Test Script ===${NC}\n"

# Function to get container names
get_containers() {
    # Support both docker-compose v1 (cbt-example-engine-1) and v2 (example-engine-1) naming
    docker ps --filter "name=engine" --format "{{.Names}}" | grep -E "engine-[0-9]" | sort
}

# Function to get leader from Redis
get_leader() {
    docker exec cbt-redis redis-cli GET "cbt:scheduler:leader" 2>/dev/null || echo "none"
}

# Function to get leader container name
get_leader_container() {
    local leader_id=$(get_leader)
    if [ "$leader_id" = "none" ]; then
        echo "none"
        return
    fi
    
    # Search recent logs (last 50 lines) for the instance ID to find which container is leader
    for container in $(get_containers); do
        if docker logs --tail 50 "$container" 2>&1 | grep -q "instance_id=$leader_id.*Promoted to leader"; then
            echo "$container"
            return
        fi
    done
    echo "unknown"
}

# Function to check scheduler entries
check_scheduler_entries() {
    echo -e "${YELLOW}Scheduler Entries in Redis:${NC}"
    docker exec cbt-redis redis-cli --scan --pattern "asynq:*scheduler*" | while read key; do
        echo "  - $key"
    done
    echo ""
}

# Function to show leader election events
show_election_events() {
    echo -e "${YELLOW}Recent Leader Election Events:${NC}"
    for container in $(get_containers); do
        echo -e "${BLUE}[$container]${NC}"
        docker logs "$container" 2>&1 | grep -E "Promoted to|Demoted from|Starting leader election" | tail -3
    done
    echo ""
}

# Main test scenarios
case "${1:-status}" in
    status)
        echo -e "${GREEN}Current Leader Election Status:${NC}\n"
        
        containers=$(get_containers)
        echo -e "${YELLOW}Running Containers:${NC}"
        echo "$containers" | nl
        echo ""
        
        leader=$(get_leader)
        echo -e "${YELLOW}Leader Lock in Redis:${NC}"
        if [ "$leader" = "none" ]; then
            echo -e "${RED}  No leader elected!${NC}"
        else
            echo -e "${GREEN}  $leader${NC}"
        fi
        echo ""
        
        leader_container=$(get_leader_container)
        echo -e "${YELLOW}Leader Container:${NC}"
        if [ "$leader_container" = "none" ]; then
            echo -e "${RED}  No leader${NC}"
        elif [ "$leader_container" = "unknown" ]; then
            echo -e "${YELLOW}  Unknown (logs not found)${NC}"
        else
            echo -e "${GREEN}  $leader_container${NC}"
        fi
        echo ""
        
        check_scheduler_entries
        show_election_events
        ;;
        
    watch)
        echo -e "${GREEN}Watching leader election (Ctrl+C to stop)...${NC}\n"
        while true; do
            clear
            echo -e "${BLUE}=== Leader Election Status ($(date +%H:%M:%S)) ===${NC}\n"
            
            leader=$(get_leader)
            echo -e "${YELLOW}Current Leader:${NC} ${GREEN}$leader${NC}"
            
            echo -e "\n${YELLOW}Container Status:${NC}"
            for container in $(get_containers); do
                status=$(docker inspect "$container" --format '{{.State.Status}}')
                if docker logs "$container" 2>&1 | tail -20 | grep -q "Promoted to leader"; then
                    echo -e "  ${GREEN}● $container${NC} (status: $status) ${GREEN}[LEADER]${NC}"
                else
                    echo -e "  ${BLUE}○ $container${NC} (status: $status)"
                fi
            done
            
            echo -e "\n${YELLOW}Recent Logs (last 3 lines):${NC}"
            for container in $(get_containers); do
                echo -e "${BLUE}[$container]${NC}"
                docker logs "$container" 2>&1 | grep -E "leader|election|Promoted|Demoted" | tail -3 | sed 's/^/  /'
            done
            
            sleep 3
        done
        ;;
        
    failover)
        echo -e "${GREEN}Testing Leader Failover...${NC}\n"
        
        leader_container=$(get_leader_container)
        if [ "$leader_container" = "none" ] || [ "$leader_container" = "unknown" ]; then
            echo -e "${RED}No leader found to stop${NC}"
            exit 1
        fi
        
        echo -e "${YELLOW}Current leader:${NC} $leader_container"
        echo -e "${YELLOW}Stopping leader container...${NC}"
        docker stop "$leader_container"
        
        echo -e "\n${YELLOW}Waiting for failover (15 seconds)...${NC}"
        for i in {15..1}; do
            echo -ne "\r  ${i}s remaining..."
            sleep 1
        done
        echo ""
        
        new_leader=$(get_leader)
        new_container=$(get_leader_container)
        
        echo -e "\n${GREEN}Failover Complete!${NC}"
        echo -e "${YELLOW}New Leader:${NC} ${GREEN}$new_leader${NC}"
        echo -e "${YELLOW}New Leader Container:${NC} ${GREEN}$new_container${NC}\n"
        
        echo -e "${YELLOW}Restarting stopped container...${NC}"
        docker start "$leader_container"
        
        echo -e "\n${GREEN}Failover test complete. Container restarted as follower.${NC}"
        ;;
        
    rolling-deploy)
        echo -e "${GREEN}Simulating Rolling Deployment...${NC}\n"
        
        containers=($(get_containers))
        
        for i in "${!containers[@]}"; do
            container="${containers[$i]}"
            is_leader=false
            
            if [ "$(get_leader_container)" = "$container" ]; then
                is_leader=true
                echo -e "${YELLOW}Container $((i+1))/${#containers[@]}:${NC} ${GREEN}$container [LEADER]${NC}"
            else
                echo -e "${YELLOW}Container $((i+1))/${#containers[@]}:${NC} $container"
            fi
            
            echo "  Restarting..."
            docker restart "$container" > /dev/null
            
            if [ "$is_leader" = true ]; then
                echo "  Was leader, waiting for failover (15s)..."
                sleep 15
            else
                echo "  Waiting for stabilization (5s)..."
                sleep 5
            fi
            
            new_leader=$(get_leader_container)
            echo -e "  Current leader: ${GREEN}$new_leader${NC}\n"
        done
        
        echo -e "${GREEN}Rolling deployment complete!${NC}"
        echo ""
        $0 status
        ;;
        
    logs)
        echo -e "${GREEN}Streaming leader election logs (Ctrl+C to stop)...${NC}\n"
        docker compose -f ../docker-compose.yml logs -f engine | grep --line-buffered -E "election|leader|Promoted|Demoted"
        ;;
        
    redis)
        echo -e "${GREEN}Redis Inspection:${NC}\n"
        
        echo -e "${YELLOW}Leader Lock:${NC}"
        docker exec cbt-redis redis-cli GET "cbt:scheduler:leader"
        echo ""
        
        echo -e "${YELLOW}Leader Lock TTL:${NC}"
        ttl=$(docker exec cbt-redis redis-cli TTL "cbt:scheduler:leader")
        echo "$ttl seconds"
        echo ""
        
        echo -e "${YELLOW}All CBT Keys:${NC}"
        docker exec cbt-redis redis-cli KEYS "cbt:*"
        echo ""
        
        echo -e "${YELLOW}Scheduler Entries:${NC}"
        docker exec cbt-redis redis-cli KEYS "asynq:*scheduler*"
        echo ""
        ;;
        
    verify)
        echo -e "${GREEN}Verifying handler registration fix...${NC}\n"
        
        echo -e "${YELLOW}Checking for 'handler not found' errors:${NC}"
        error_count=$(docker compose logs engine 2>&1 | grep -i "handler not found" | wc -l)
        if [ "$error_count" -eq 0 ]; then
            echo -e "${GREEN}  ✓ No handler errors found!${NC}"
        else
            echo -e "${RED}  ✗ Found $error_count handler errors${NC}"
            echo -e "\n${YELLOW}Sample errors:${NC}"
            docker compose logs engine 2>&1 | grep -i "handler not found" | head -5
        fi
        echo ""
        
        echo -e "${YELLOW}Verifying scheduled task execution:${NC}"
        task_count=$(docker compose logs engine 2>&1 | grep -E "Processing scheduled (forward|backfill)" | wc -l)
        echo -e "  Tasks processed: ${GREEN}$task_count${NC}"
        echo ""
        
        echo -e "${YELLOW}Recent scheduled task executions:${NC}"
        docker compose logs engine 2>&1 | grep -E "Processing scheduled|Model execution completed" | tail -10 | sed 's/^/  /'
        echo ""
        
        echo -e "${YELLOW}Handler registration on all instances:${NC}"
        for container in $(get_containers); do
            handler_count=$(docker logs "$container" 2>&1 | grep "Registering" | wc -l)
            echo -e "  ${BLUE}$container${NC}: ${GREEN}$handler_count handlers registered${NC}"
        done
        ;;
        
    help|*)
        echo "Usage: $0 {status|watch|failover|rolling-deploy|logs|redis|verify|help}"
        echo ""
        echo "Commands:"
        echo "  status          - Show current leader election status"
        echo "  watch           - Watch leader election status in real-time"
        echo "  failover        - Test leader failover by stopping current leader"
        echo "  rolling-deploy  - Simulate a rolling deployment"
        echo "  logs            - Stream leader election logs"
        echo "  redis           - Inspect Redis keys and leader lock"
        echo "  verify          - Verify handler registration fix and task execution"
        echo "  help            - Show this help message"
        echo ""
        echo "Monitoring UIs:"
        echo "  Asynqmon:        http://localhost:8080"
        echo "  Redis Commander: http://localhost:8081"
        ;;
esac