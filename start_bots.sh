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

# Build the command chain to run scripts one after another
run_chain=""
for script_path in "${scripts[@]}"; do
    script_name=$(basename "$script_path")
    run_chain+="echo '▶️ Starting $script_name at \$(date)'; "
    run_chain+="python3 $script_path; "
    run_chain+="echo '✅ Finished $script_name at \$(date)'; "
    run_chain+="echo '----------------------------------------'; "
done

# Add a completion message
run_chain+="echo '🎉 All scripts completed successfully at \$(date)!'; exec bash"

# Send the command chain to tmux (evaluate inside session)
tmux send-keys -t "$sessname":0 "$run_chain" Enter

# Info to user
date
echo "Started sequential execution of ${#scripts[@]} scripts in tmux session: $sessname"
echo "Attach anytime with: tmux attach -t $sessname"
