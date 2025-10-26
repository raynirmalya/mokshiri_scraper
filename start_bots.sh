#!/bin/bash

sessname="mokshiri"
scripts=(
    "/root/code/mokshiri-scrapes/scrapers/kbizoom_scraper.py"
)

# Create session
tmux new-session -d -s "$sessname"

# Run each script in separate windows
for i in "${!scripts[@]}"; do
    script_path="${scripts[$i]}"
    script_name=$(basename "$script_path" .py)
    
    if [ $i -eq 0 ]; then
        # First script uses initial window
        tmux rename-window -t "$sessname":0 "$script_name"
        tmux send-keys -t "$sessname":0 "python3 $script_path" Enter
    else
        # Additional scripts get new windows
        tmux new-window -t "$sessname":$i -n "$script_name"
        tmux send-keys -t "$sessname":$i "python3 $script_path" Enter
    fi
done

date
echo "Started ${#scripts[@]} scripts in tmux session: $sessname"
echo "Attach with: tmux attach -t $sessname"
