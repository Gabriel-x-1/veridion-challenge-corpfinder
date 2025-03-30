"""
Test script for Company Profile Matcher API

This script:
1. Reads the API input sample CSV
2. Sends requests to the API for matching
3. Calculates and reports the match rate
4. Visualizes the match results
"""

import csv
import json
import requests
import pandas as pd
from tqdm import tqdm
import os
import matplotlib.pyplot as plt
from datetime import datetime

# API endpoint
API_URL = "http://localhost:5000/api/match"

def create_visualizations(results, match_rate, test_type):
    """Create and save visualizations for the matching results"""
    try:
        # Create plots directory
        os.makedirs('results/test-api/plots', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Set style - use default style instead of seaborn
        plt.style.use('default')
        
        # Set general plot parameters
        plt.rcParams['figure.figsize'] = (10, 6)
        plt.rcParams['axes.grid'] = True
        
        # 1. Match Rate Pie Chart
        plt.figure()
        colors = ['#2ecc71', '#e74c3c']
        plt.pie([match_rate, 100-match_rate], 
                labels=['Matched', 'Unmatched'],
                autopct='%1.1f%%',
                colors=colors,
                startangle=90)
        plt.title(f'{test_type} - Match Rate Distribution')
        plt.savefig(f'results/test-api/plots/{test_type.lower()}_match_rate_{timestamp}.png')
        plt.close()
        
        # For individual matches, create additional visualizations
        if test_type == 'Individual Matches' and results:
            # 2. Input Fields Distribution
            plt.figure()
            input_fields = []
            for result in results:
                input_fields.extend(result['input'].keys())
            field_counts = pd.Series(input_fields).value_counts()
            
            plt.bar(range(len(field_counts)), field_counts.values)
            plt.xticks(range(len(field_counts)), field_counts.index, rotation=45)
            plt.title('Distribution of Input Fields')
            plt.xlabel('Field Name')
            plt.ylabel('Count')
            plt.tight_layout()
            plt.savefig(f'results/test-api/plots/input_fields_distribution_{timestamp}.png')
            plt.close()
            
    except Exception as e:
        print(f"Warning: Could not create visualizations: {str(e)}")
        print("Continuing with other operations...")

def test_individual_matches(csv_file):
    """Test individual matching via the API"""
    results = []
    matches = 0
    total = 0
    
    # Load CSV data
    df = pd.read_csv(csv_file)
    
    print(f"Testing {len(df)} company records...")
    
    # Process each row
    for _, row in tqdm(df.iterrows(), total=len(df)):
        total += 1
        
        # Prepare request data
        data = {
            "name": row.get("input name"),
            "website": row.get("input website"),
            "phone": row.get("input phone"),
            "facebook": row.get("input_facebook")
        }
        
        # Filter out None values
        data = {k: v for k, v in data.items() if pd.notna(v)}
        
        # Make API request
        response = requests.post(API_URL, json=data)
        
        # Process response
        result = {
            "input": data,
            "matched": False,
            "match": None
        }
        
        if response.status_code == 200:
            api_result = response.json()
            if api_result["status"] == "success":
                result["matched"] = True
                result["match"] = api_result["match"]
                matches += 1
        
        results.append(result)
    
    # Calculate match rate
    match_rate = (matches / total) * 100 if total > 0 else 0
    
    print(f"Match rate: {match_rate:.2f}%")
    print(f"Matched {matches} out of {total} companies")
    
    # Add visualization
    create_visualizations(results, match_rate, "Individual Matches")
    
    return results, match_rate

def test_bulk_match(csv_file):
    """Test bulk matching via the API"""
    # Load CSV data
    data = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                "name": row.get("input name"),
                "website": row.get("input website"),
                "phone": row.get("input phone"),
                "facebook": row.get("input_facebook")
            })
    
    # Make bulk API request
    bulk_url = "http://localhost:5000/api/bulk-match"
    response = requests.post(bulk_url, json=data)
    
    if response.status_code == 200:
        result = response.json()
        match_count = result["match_count"]
        total_count = result["total_count"]
        match_rate = (match_count / total_count) * 100 if total_count > 0 else 0
        
        print(f"Bulk match rate: {match_rate:.2f}%")
        print(f"Matched {match_count} out of {total_count} companies")
        
        # Save detailed results to file in results/test-api folder
        with open('results/test-api/bulk_match_results.json', 'w') as f:
            json.dump(result, f, indent=2)
            
        # Add visualization
        create_visualizations(None, match_rate, "Bulk Matches")
        
        print("Detailed results saved to results/test-api/bulk_match_results.json")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

def test_csv_processor():
    """Test the CSV processor endpoint"""
    # Create nested results directory if it doesn't exist
    os.makedirs('results/test-api', exist_ok=True)
    
    files = {'file': open('data/API-input-sample.csv', 'rb')}
    response = requests.post('http://localhost:5000/api/process-csv', files=files)
    
    if response.status_code == 200:
        result = response.json()
        print(f"CSV processing match rate: {result['match_rate']}")
        print(f"Matched {result['matched_count']} out of {result['total_count']} companies")
        
        # Save detailed results to file in results/test-api folder
        with open('results/test-api/csv_process_results.json', 'w') as f:
            json.dump(result, f, indent=2)
            
        # Add visualization
        create_visualizations(None, result['match_rate'], "CSV Processing")
        
        print("Detailed results saved to results/test-api/csv_process_results.json")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    # Create nested results directory if it doesn't exist
    os.makedirs('results/test-api', exist_ok=True)
    
    # Test individual matches
    print("Testing individual API matches...")
    results, rate = test_individual_matches("data/API-input-sample.csv")
    
    # Save results in results/test-api folder
    with open('results/test-api/match_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Test bulk matching
    print("\nTesting bulk API matching...")
    test_bulk_match("data/API-input-sample.csv")
    
    # Test CSV processor
    print("\nTesting CSV processor API...")
    test_csv_processor()