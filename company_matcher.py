import os
import re
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import tldextract
from urllib.parse import urlparse
import Levenshtein
import warnings
import ast

# Suppress pandas warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Configure Elasticsearch - adapt connection parameters based on your environment
ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_HOST', 'localhost')
ELASTICSEARCH_PORT = os.environ.get('ELASTICSEARCH_PORT', 9200)
ELASTICSEARCH_USERNAME = os.environ.get('ELASTICSEARCH_USERNAME', '')
ELASTICSEARCH_PASSWORD = os.environ.get('ELASTICSEARCH_PASSWORD', '')

# Define the index name
INDEX_NAME = 'company_profiles'

# Connection to Elasticsearch
if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD:
    es = Elasticsearch([f'http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}'],
                     basic_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD))
else:
    es = Elasticsearch([f'http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}'])


class CompanyProfileMatcher:
    def __init__(self, es_client):
        self.es = es_client
    
    def prepare_data(self, scraped_data_file, company_names_file, output_file=None):
        """
        Merge scraped company data with company names
        
        Args:
            scraped_data_file: Path to scraped company data CSV
            company_names_file: Path to company names CSV
            output_file: Optional path to save merged data CSV
            
        Returns:
            DataFrame with merged company data
        """
        print(f"Loading scraped data from {scraped_data_file}")
        scraped_df = pd.read_csv(scraped_data_file)
        
        print(f"Loading company names from {company_names_file}")
        names_df = pd.read_csv(company_names_file)
        
        # Clean and join the data on domain
        scraped_df['domain'] = scraped_df['domain'].str.lower().str.strip()
        names_df['domain'] = names_df['domain'].str.lower().str.strip()
        
        # Merge the dataframes on domain
        merged_df = pd.merge(scraped_df, names_df, on='domain', how='left')
        print(f"Merged data shape: {merged_df.shape}")
        
        # Fill missing company names with domain
        missing_name_mask = merged_df['company_commercial_name'].isna()
        merged_df.loc[missing_name_mask, 'company_commercial_name'] = merged_df.loc[missing_name_mask, 'domain']
        merged_df.loc[missing_name_mask, 'company_legal_name'] = merged_df.loc[missing_name_mask, 'domain']
        
        # Clean phone numbers (convert from string representation of list to actual list)
        merged_df['phones'] = merged_df['phones'].apply(self._clean_string_list)
        
        # Clean social media links
        for social_col in ['facebook_links', 'twitter_links', 'instagram_links', 'linkedin_links', 'youtube_links']:
            if social_col in merged_df.columns:
                merged_df[social_col] = merged_df[social_col].apply(self._clean_string_list)
        
        # Clean addresses
        if 'addresses' in merged_df.columns:
            merged_df['addresses'] = merged_df['addresses'].apply(self._clean_string_list)
        
        # Save to file if output path is provided
        if output_file:
            merged_df.to_csv(output_file, index=False)
            print(f"Merged data saved to {output_file}")
        
        return merged_df
    
    def _clean_string_list(self, value):
        """Convert string representation of list to actual list"""
        if pd.isna(value) or value == '[]':
            return []
        
        try:
            # Try to parse as literal list
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            # If not a valid list, return as a single item list
            return [value]
    
    def _extract_domain(self, url):
        """Extract domain from URL"""
        if not url:
            return None
            
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
            
        # Extract domain
        try:
            extracted = tldextract.extract(url)
            return f"{extracted.domain}.{extracted.suffix}".lower()
        except:
            # Fallback
            try:
                parsed = urlparse(url)
                return parsed.netloc.lower()
            except:
                return url.lower()
    
    def _normalize_facebook_url(self, url):
        """Normalize Facebook URL format"""
        if not url:
            return None
        
        # Remove scheme and www
        url = re.sub(r'^https?://(www\.)?', '', url.lower())
        
        # Extract Facebook username or page ID
        facebook_patterns = [
            r'facebook\.com/([a-zA-Z0-9\.\-\_]+)/?.*',
            r'fb\.com/([a-zA-Z0-9\.\-\_]+)/?.*',
            r'facebook\.com/profile\.php\?id=([0-9]+)/?.*'
        ]
        
        for pattern in facebook_patterns:
            match = re.match(pattern, url)
            if match:
                return match.group(1).lower()
        
        return url.lower()
    
    def _normalize_phone(self, phone):
        """Normalize phone number format"""
        if not phone:
            return None
        
        # Remove all non-digit characters except + at the beginning
        normalized = re.sub(r'[^\d+]', '', phone)
        
        # Remove leading + if present
        if normalized.startswith('+'):
            normalized = normalized[1:]
        
        # Keep only the last 10 digits for comparison
        if len(normalized) > 10:
            normalized = normalized[-10:]
            
        return normalized
    
    def create_es_index(self):
        """Create Elasticsearch index with appropriate mappings"""
        if self.es.indices.exists(index=INDEX_NAME):
            print(f"Index {INDEX_NAME} already exists. Deleting...")
            self.es.indices.delete(index=INDEX_NAME)
        
        # Define mappings for company profile data
        mappings = {
            "mappings": {
                "properties": {
                    "company_id": {"type": "keyword"},
                    "website": {"type": "keyword"},
                    "domain": {"type": "keyword"},
                    "company_commercial_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "completion": {"type": "completion"}
                        }
                    },
                    "company_legal_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "company_all_names": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "phones": {"type": "keyword"},
                    "phones_normalized": {"type": "keyword"},
                    "addresses": {"type": "text"},
                    "facebook_links": {
                        "type": "keyword",
                        "fields": {
                            "normalized": {"type": "keyword"}
                        }
                    },
                    "twitter_links": {"type": "keyword"},
                    "instagram_links": {"type": "keyword"},
                    "linkedin_links": {"type": "keyword"},
                    "youtube_links": {"type": "keyword"}
                }
            },
            "settings": {
                "analysis": {
                    "analyzer": {
                        "company_name_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding"]
                        }
                    }
                }
            }
        }
        
        # Create the index
        self.es.indices.create(index=INDEX_NAME, body=mappings)
        print(f"Created Elasticsearch index: {INDEX_NAME}")
    
    def index_company_data(self, df):
        """Index company data into Elasticsearch"""
        print(f"Preparing to index {len(df)} company profiles...")
        
        # Prepare documents for bulk indexing
        def generate_docs():
            for i, row in df.iterrows():
                try:
                    # Convert row to dict and handle missing values
                    if isinstance(row, pd.Series):
                        row_dict = row.to_dict()
                    else:
                        row_dict = dict(row)
                    
                    # Basic document structure with safe fallbacks
                    doc = {
                        "company_id": str(i),  # Convert to string to be safe
                        "website": str(row_dict.get('website', '')),
                        "domain": str(row_dict.get('domain', '')).lower().strip(),
                        "company_commercial_name": str(row_dict.get('company_commercial_name', '')),
                        "company_legal_name": str(row_dict.get('company_legal_name', '')),
                        "status": str(row_dict.get('status', 'unknown'))
                    }
                    
                    # Handle company names - ensure they're strings
                    if pd.isna(doc["company_commercial_name"]) or doc["company_commercial_name"] == '':
                        doc["company_commercial_name"] = doc["domain"]
                    
                    if pd.isna(doc["company_legal_name"]) or doc["company_legal_name"] == '':
                        doc["company_legal_name"] = doc["company_commercial_name"]
                    
                    # Safely handle company_all_names
                    all_names = row_dict.get('company_all_available_names', '')
                    doc["company_all_names"] = str(all_names) if not pd.isna(all_names) else doc["company_commercial_name"]
                    
                    # Handle phones - ensure it's a list of strings
                    try:
                        phones = row_dict.get('phones', [])
                        if isinstance(phones, list):
                            doc["phones"] = [str(p) for p in phones if p]
                        elif isinstance(phones, str):
                            if phones.startswith('[') and phones.endswith(']'):
                                # Try to parse as a string representation of a list
                                try:
                                    parsed_phones = ast.literal_eval(phones)
                                    if isinstance(parsed_phones, list):
                                        doc["phones"] = [str(p) for p in parsed_phones if p]
                                    else:
                                        doc["phones"] = []
                                except:
                                    doc["phones"] = []
                            else:
                                # Single phone as string
                                doc["phones"] = [phones] if phones else []
                        else:
                            doc["phones"] = []
                    except:
                        doc["phones"] = []
                    
                    # Normalize phone numbers
                    doc["phones_normalized"] = []
                    for phone in doc["phones"]:
                        try:
                            normalized = self._normalize_phone(phone)
                            if normalized:
                                doc["phones_normalized"].append(normalized)
                        except:
                            # Skip problematic phone numbers
                            pass
                    
                    # Handle addresses - ensure it's a list of strings
                    try:
                        addresses = row_dict.get('addresses', [])
                        if isinstance(addresses, list):
                            doc["addresses"] = [str(a) for a in addresses if a]
                        elif isinstance(addresses, str):
                            if addresses.startswith('[') and addresses.endswith(']'):
                                # Try to parse as a string representation of a list
                                try:
                                    parsed_addresses = ast.literal_eval(addresses)
                                    if isinstance(parsed_addresses, list):
                                        doc["addresses"] = [str(a) for a in parsed_addresses if a]
                                    else:
                                        doc["addresses"] = []
                                except:
                                    doc["addresses"] = []
                            else:
                                # Single address as string
                                doc["addresses"] = [addresses] if addresses else []
                        else:
                            doc["addresses"] = []
                    except:
                        doc["addresses"] = []
                    
                    # Handle social media links
                    for social in ['facebook_links', 'twitter_links', 'instagram_links', 'linkedin_links', 'youtube_links']:
                        try:
                            social_links = row_dict.get(social, [])
                            if isinstance(social_links, list):
                                doc[social] = [str(s) for s in social_links if s]
                            elif isinstance(social_links, str):
                                if social_links.startswith('[') and social_links.endswith(']'):
                                    # Try to parse as a string representation of a list
                                    try:
                                        parsed_links = ast.literal_eval(social_links)
                                        if isinstance(parsed_links, list):
                                            doc[social] = [str(s) for s in parsed_links if s]
                                        else:
                                            doc[social] = []
                                    except:
                                        doc[social] = []
                                else:
                                    # Single link as string
                                    doc[social] = [social_links] if social_links else []
                            else:
                                doc[social] = []
                        except:
                            doc[social] = []
                    
                    # Add normalized facebook links
                    doc['facebook_links_normalized'] = []
                    for url in doc.get('facebook_links', []):
                        try:
                            normalized = self._normalize_facebook_url(url)
                            if normalized:
                                doc['facebook_links_normalized'].append(normalized)
                        except:
                            # Skip problematic URLs
                            pass
                    
                    yield {
                        "_index": INDEX_NAME,
                        "_id": str(i),
                        "_source": doc
                    }
                except Exception as e:
                    print(f"Error processing document {i}: {str(e)}")
                    continue
        
        # Use bulk helper to index documents with error handling
        try:
            success, errors = bulk(self.es, generate_docs(), chunk_size=100, max_retries=3, 
                                 raise_on_error=False, raise_on_exception=False)
            print(f"Indexed {success} documents with {len(errors)} errors")
            
            # If there are errors, display some examples for troubleshooting
            if errors:
                print("Sample errors:")
                for i, error in enumerate(errors[:5]):
                    print(f"Error {i+1}: {error}")
                
            # Even with errors, we proceed as long as we have some documents indexed
            if success > 0:
                # Refresh the index to make documents searchable immediately
                self.es.indices.refresh(index=INDEX_NAME)
                print(f"Index refreshed with {success} documents")
                return True
            else:
                print("No documents were successfully indexed.")
                return False
                
        except Exception as e:
            print(f"Bulk indexing failed with error: {str(e)}")
            
            # Try indexing documents one by one for better error visibility
            print("Attempting to index documents individually...")
            success_count = 0
            
            for i, row in df.iterrows():
                try:
                    doc = {
                        "company_id": str(i),
                        "website": str(row.get('website', '')),
                        "domain": str(row.get('domain', '')).lower().strip(),
                        # Include minimal viable document to ensure indexing works
                    }
                    
                    self.es.index(index=INDEX_NAME, id=str(i), document=doc)
                    success_count += 1
                    
                    if success_count % 100 == 0:
                        print(f"Indexed {success_count} documents individually")
                        
                except Exception as doc_err:
                    if success_count < 10:  # Show first few errors only
                        print(f"Error indexing document {i}: {str(doc_err)}")
                    continue
            
            if success_count > 0:
                print(f"Successfully indexed {success_count} documents individually")
                self.es.indices.refresh(index=INDEX_NAME)
                return True
            else:
                print("Failed to index any documents.")
                return False
    
    def match_company(self, name=None, website=None, phone=None, facebook=None):
        """
        Match company profile based on provided information
        
        Args:
            name: Company name
            website: Company website
            phone: Company phone number
            facebook: Facebook URL
            
        Returns:
            Best matching company profile
        """
        # Track scores for multiple possible matches
        match_scores = {}
        results = []
        
        # Immediate exact match by domain if website is provided
        if website:
            domain = self._extract_domain(website)
            if domain:
                domain_query = {
                    "term": {
                        "domain": domain
                    }
                }
                domain_response = self.es.search(index=INDEX_NAME, query=domain_query, size=5)
                
                for hit in domain_response['hits']['hits']:
                    company_id = hit['_source']['company_id']
                    # Domain match is a strong signal - high weight
                    match_scores[company_id] = match_scores.get(company_id, 0) + 10
                    results.append(hit['_source'])
        
        # Phone number match
        if phone:
            normalized_phone = self._normalize_phone(phone)
            if normalized_phone:
                phone_query = {
                    "match": {
                        "phones_normalized": normalized_phone
                    }
                }
                phone_response = self.es.search(index=INDEX_NAME, query=phone_query, size=5)
                
                for hit in phone_response['hits']['hits']:
                    company_id = hit['_source']['company_id']
                    # Phone match is a strong signal - high weight
                    match_scores[company_id] = match_scores.get(company_id, 0) + 8
                    results.append(hit['_source'])
        
        # Facebook URL match
        if facebook:
            normalized_fb = self._normalize_facebook_url(facebook)
            if normalized_fb:
                fb_query = {
                    "match": {
                        "facebook_links_normalized": normalized_fb
                    }
                }
                fb_response = self.es.search(index=INDEX_NAME, query=fb_query, size=5)
                
                for hit in fb_response['hits']['hits']:
                    company_id = hit['_source']['company_id']
                    # Facebook match is a good signal - medium weight
                    match_scores[company_id] = match_scores.get(company_id, 0) + 6
                    results.append(hit['_source'])
        
        # Name fuzzy match
        if name:
            # Use a more advanced query with fuzzy matching
            name_query = {
                "bool": {
                    "should": [
                        {"match": {"company_commercial_name": {"query": name, "fuzziness": "AUTO"}}},
                        {"match": {"company_legal_name": {"query": name, "fuzziness": "AUTO"}}},
                        {"match": {"company_all_names": {"query": name, "fuzziness": "AUTO"}}}
                    ]
                }
            }
            name_response = self.es.search(index=INDEX_NAME, query=name_query, size=10)
            
            for hit in name_response['hits']['hits']:
                company_id = hit['_source']['company_id']
                source = hit['_source']
                
                # Calculate Levenshtein distance for name similarity
                best_name_match = 0
                for field in ['company_commercial_name', 'company_legal_name']:
                    if field in source and source[field]:
                        similarity = 1 - (Levenshtein.distance(name.lower(), source[field].lower()) / 
                                        max(len(name), len(source[field])))
                        best_name_match = max(best_name_match, similarity)
                
                # Scale name matching score from 0-5 based on similarity
                name_score = best_name_match * 5
                match_scores[company_id] = match_scores.get(company_id, 0) + name_score
                results.append(hit['_source'])
        
        # If no matches found through specific fields, perform a multi-match query
        if not match_scores:
            fields = []
            query_parts = {}
            
            if name:
                fields.extend(["company_commercial_name^3", "company_legal_name^2", "company_all_names"])
                query_parts["name"] = name
            
            if website:
                fields.append("website")
                query_parts["website"] = website
            
            if phone:
                fields.append("phones")
                query_parts["phone"] = phone
            
            if facebook:
                fields.append("facebook_links")
                query_parts["facebook"] = facebook
            
            if fields and query_parts:
                query_string = " ".join(query_parts.values())
                fallback_query = {
                    "multi_match": {
                        "query": query_string,
                        "fields": fields,
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                }
                
                fallback_response = self.es.search(index=INDEX_NAME, query=fallback_query, size=10)
                for hit in fallback_response['hits']['hits']:
                    company_id = hit['_source']['company_id']
                    match_scores[company_id] = match_scores.get(company_id, 0) + hit['_score'] / 10
                    results.append(hit['_source'])
        
        # Get best match
        if match_scores:
            best_match_id = max(match_scores.items(), key=lambda x: x[1])[0]
            
            # Find the corresponding result
            best_match = None
            for result in results:
                if result['company_id'] == best_match_id:
                    best_match = result
                    break
            
            if best_match:
                # Add the score for transparency
                best_match['match_score'] = match_scores[best_match_id]
                return best_match
        
        return None

def find_valid_file_path(possible_paths):
    """Helper to find the first valid file from a list of possible paths"""
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None

def get_matcher():
    """Function to get a configured CompanyProfileMatcher instance"""
    return CompanyProfileMatcher(es)

def setup_elasticsearch_index():
    """Setup the Elasticsearch index with company data"""
    # Initialize the matcher
    matcher = CompanyProfileMatcher(es)
    
    try:
        # Check if input files exist in various possible locations
        scraped_data_paths = [
            'results/scraped_company_data.csv' 
        ]
        
        company_names_paths = [
            'data/sample-websites-company-names.csv'
        ]
        
        # Find the first valid scraped data file
        scraped_data_file = find_valid_file_path(scraped_data_paths)
                
        if not scraped_data_file:
            print("ERROR: Could not find scraped company data file. Please ensure it exists in the current directory or in a 'data' or 'results' subdirectory.")
            return False
            
        # Find the first valid company names file
        company_names_file = find_valid_file_path(company_names_paths)
                
        if not company_names_file:
            print("ERROR: Could not find company names data file. Please ensure it exists in the current directory or in a 'data' subdirectory.")
            return False
        
        print(f"Using scraped data file: {scraped_data_file}")
        print(f"Using company names file: {company_names_file}")
            
        # Merge the data
        merged_data = matcher.prepare_data(
            scraped_data_file=scraped_data_file,
            company_names_file=company_names_file,
            output_file='merged_company_profiles.csv'
        )
        
        # Create the Elasticsearch index
        matcher.create_es_index()
        
        # Index the merged data
        indexing_success = matcher.index_company_data(merged_data)
        
        return indexing_success
    
    except Exception as e:
        print(f"Error during setup: {str(e)}")
        return False