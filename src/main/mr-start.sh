sock=sock123
n_workers=5
app=wc
# app=indexer

rm mr-*.json tmp* $sock

# Rebuild plugin
go build -buildmode=plugin ../mrapps/$app.go

# Start coordinator in BACKGROUND
go run mrcoordinator.go $sock pg-*.txt 2>&1 &
COORD_PID=$!

# Give coordinator a moment to start
sleep 1

# Start workers in BACKGROUND
for ((i=1; i<=n_workers; i++)); do
    go run mrworker.go $app.so $sock 2>&1 &
done

# Wait for all background jobs (workers + coordinator)
wait