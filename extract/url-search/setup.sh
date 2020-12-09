#!/bin/bash

# Create and initialize a Python Virtual Environment
echo "Creating virtual env - .venv"
python3 -m venv .venv

echo "sourcing virtual env - .venv"
source .venv/bin/activate

# Create a directory to put things in
echo "Creating 'setup' directory"
mkdir setup

# Move the relevant files into setup directory
echo "Moving function file(s) to setup dir"
cp url_search.py setup/
cd ./setup

# Install requirements 
echo "pip installing requirements from requirements file in target directory"
pip install -r ../requirements.txt -t .

# Prepares the deployment package
echo "Zipping package"
zip -r ../package.zip ./* 

# Remove the setup directory used
echo "Removing setup directory and virtual environment"
cd ..
rm -rf ./setup
deactivate
rm -rf ./.venv