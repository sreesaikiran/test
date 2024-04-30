#!/bin/bash

# Function to destroy Terraform resources in a given directory
destroy_terraform() {
    local dir="$1"
    echo "Destroying Terraform resources in $dir"
    cd "$dir"
    terraform init -input=false && terraform destroy -auto-approve
    cd - > /dev/null
}

export -f destroy_terraform

# Main script starts here
# Replace "/path/to/search/root" with the root directory you want to start the search from
find /path/to/search/root -type f -name "*.tf" -print0 | while IFS= read -r -d '' file; do
    # Extract the directory from the path of the .tf file
    dir=$(dirname "$file")
    # Call destroy_terraform function uniquely on directories
    destroy_terraform "$dir"
done | sort -u
