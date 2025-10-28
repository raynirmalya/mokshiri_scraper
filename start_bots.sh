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

# Kill any existing session with same name
tmux has-session -t "$sessname" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "Killing existing tmux session: $sessname"
    tmux kill-session -t "$sessname"
fi

# Create new tmux session (detached)
tmux new-session -d -s "$sessname" -n "runner"

# Build the command chain to run scripts one by one
run_chain=""
for script_path in "${scripts[@]}"; do
    run_chain+="python3 $script_path; "
done
# Add a message when done
run_chain+="echo '✅ All scripts completed successfully!'; exec bash"

# Send the combined command chain to tmux
tmux send-keys -t "$sessname":0 "$run_chain" Enter

date
echo "Started sequential execution of ${#scripts[@]} scripts in tmux session: $sessname"
echo "Attach anytime with: tmux attach -t $sessname"
