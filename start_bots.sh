#!/bin/bash

sessname="mokshiri"
scripts=(
    "/root/code/mokshiri_scraper/scrapers/kbizoom_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/kdramastars_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/kpopmart_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/kheralds_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/koreatech_startup_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/kpopmap_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/soomi_scraper.py"
    "/root/code/mokshiri_scraper/scrapers/thepicktool_scraper.py"
    "/root/code/mokshiri_scraper/batch_watermark_r2.py"
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
