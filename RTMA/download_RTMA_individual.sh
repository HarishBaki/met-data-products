#!/bin/bash

# Variables to be passed to Python script
VARIABLE="PCP"
LEVEL="surface"
DOWNLOAD_PATH="RTMA/$VARIABLE"

target_file_count=24

# Initialize an array to store folder names
invalid_folders=()

# Loop through each subfolder in the root folder
for folder in "$DOWNLOAD_PATH"/rtma/*; do
    if [ -d "$folder" ]; then
        # Count the number of files in the subfolder (ignores hidden files)
        file_count=$(find "$folder" -type f | wc -l)
        
        # Check if the file count is not equal to the target file count
        if [ "$file_count" -ne "$target_file_count" ]; then
            echo "$(basename "$folder")"
            invalid_folders+=("$(basename "$folder")")
        fi
    fi
done

# Access the collected folders
echo "Folders with invalid file counts:"
printf "%s\n" "${invalid_folders[@]}"


# Loop through each date
for date in "${invalid_folders[@]}"; do
    # Extract year, month, and day using substring
    year=${date:0:4}
    month=${date:4:2}
    day=${date:6:2}
    
    for hour in {00..23}; do
	# Call the Python script with the arguments
	python3 RTMA_download_variable_and_instancewise.py "$VARIABLE" "$LEVEL" "$year" "$month" "$day" "$hour" "$DOWNLOAD_PATH" &
    done
    wait
done
wait
echo "All downloads have been finished."
