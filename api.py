"""
Company Profile Matching API

This application:
1. Provides a REST API for company profile matching
2. Implements a scoring algorithm to find the best match

Usage:
- First set up the Elasticsearch index by running the setup_elasticsearch_index function
- Then run this script to start the API server
"""

import os
import csv
from flask import Flask, request, jsonify
import company_matcher

app = Flask(__name__)

# Get a configured matcher instance
matcher = company_matcher.get_matcher()

@app.route('/api/match', methods=['POST'])
def match_company_api():
    """API endpoint to match a company profile"""
    data = request.get_json()
    
    # Get parameters
    name = data.get('name')
    website = data.get('website')
    phone = data.get('phone')
    facebook = data.get('facebook')
    
    # Check if at least one parameter is provided
    if not any([name, website, phone, facebook]):
        return jsonify({
            'status': 'error',
            'message': 'At least one of name, website, phone, or facebook must be provided'
        }), 400
    
    # Match the company
    match = matcher.match_company(name, website, phone, facebook)
    
    if match:
        return jsonify({
            'status': 'success',
            'match': match
        })
    else:
        return jsonify({
            'status': 'not_found',
            'message': 'No matching company profile found'
        }), 404

@app.route('/api/bulk-match', methods=['POST'])
def bulk_match_companies():
    """API endpoint to match multiple companies"""
    data = request.get_json()
    
    if not isinstance(data, list):
        return jsonify({
            'status': 'error',
            'message': 'Input must be a list of company data objects'
        }), 400
    
    results = []
    
    for company_data in data:
        name = company_data.get('name')
        website = company_data.get('website')
        phone = company_data.get('phone')
        facebook = company_data.get('facebook')
        
        match = matcher.match_company(name, website, phone, facebook)
        
        results.append({
            'input': company_data,
            'match': match if match else None
        })
    
    return jsonify({
        'status': 'success',
        'match_count': sum(1 for r in results if r['match']),
        'total_count': len(results),
        'results': results
    })

def process_csv_file(input_file):
    """Process a CSV file and return matches"""
    results = []
    matched_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            name = row.get('input name')
            website = row.get('input website')
            phone = row.get('input phone')
            facebook = row.get('input_facebook')
            
            match = matcher.match_company(name, website, phone, facebook)
            
            result = {
                'input': {
                    'name': name,
                    'website': website,
                    'phone': phone,
                    'facebook': facebook
                },
                'match': match
            }
            
            if match:
                matched_count += 1
                
            results.append(result)
    
    return results, matched_count

@app.route('/api/process-csv', methods=['POST'])
def process_csv_api():
    """API endpoint to process a CSV file"""
    if 'file' not in request.files:
        return jsonify({
            'status': 'error',
            'message': 'No file provided'
        }), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({
            'status': 'error',
            'message': 'No file selected'
        }), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({
            'status': 'error',
            'message': 'File must be a CSV'
        }), 400
    
    # Save the uploaded file temporarily
    temp_path = 'temp_upload.csv'
    file.save(temp_path)
    
    # Process the file
    try:
        results, matched_count = process_csv_file(temp_path)
        
        # Generate a detailed report
        return jsonify({
            'status': 'success',
            'match_rate': f"{(matched_count / len(results) * 100):.2f}%",
            'matched_count': matched_count,
            'total_count': len(results),
            'results': results
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f"Error processing CSV: {str(e)}"
        }), 500
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)

def run_test_match():
    """Run a test match with sample data"""
    test_input_paths = [
        'data/API-input-sample.csv'
    ]
    
    # Find the first valid test input file
    test_input = company_matcher.find_valid_file_path(test_input_paths)
        
    if test_input:
        print(f"Running test match with sample data from {test_input}...")
        try:
            results, matched_count = process_csv_file(test_input)
            
            print(f"Match rate: {(matched_count / len(results) * 100):.2f}%")
            print(f"Matched {matched_count} out of {len(results)} companies")
            return True
        except Exception as e:
            print(f"Error running test match: {str(e)}")
            return False
    else:
        print("No test input file found. Skipping test match.")
        return False

if __name__ == '__main__':
    # Check if the Elasticsearch index is already set up
    print("Starting Company Profile Matching API...")
    
    # Check if we already have documents in the index
    if not matcher.es.indices.exists(index=company_matcher.INDEX_NAME):
        print("Elasticsearch index does not exist. Setting up...")
        success = company_matcher.setup_elasticsearch_index()
        if not success:
            print("WARNING: Setup was not completely successful. API may have limited functionality.")
    else:
        # Run a quick test to verify connection
        try:
            count = matcher.es.count(index=company_matcher.INDEX_NAME)
            print(f"Elasticsearch index exists with {count['count']} documents.")
            
            # Optional test match
            run_test_match()
        except Exception as e:
            print(f"Error connecting to Elasticsearch: {str(e)}")
            print("WARNING: API may not function correctly without Elasticsearch connection.")
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
    print(f"API server running on port {port}")