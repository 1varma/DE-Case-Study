from elasticsearch import Elasticsearch

# Elasticsearch configuration
es = Elasticsearch("http://localhost:9200")

# Function to perform analysis on the stored data
def perform_analysis():
    # Elasticsearch aggregation query
    aggregation = {
        "aggs": {
            "clicks_sum": {"sum": {"field": "clicks"}},
            "conversions_sum": {"sum": {"field": "conversions"}},
            "clicks_stats": {"stats": {"field": "clicks"}},
            "conversions_stats": {"stats": {"field": "conversions"}}
        }
    }
    # Perform search with aggregation
    result = es.search(index='clicks_conversions_index', body={"aggs": aggregation})
    # Extract aggregated data and statistical metrics
    clicks_sum = result['aggregations']['clicks_sum']['value']
    conversions_sum = result['aggregations']['conversions_sum']['value']
    clicks_stats = result['aggregations']['clicks_stats']
    conversions_stats = result['aggregations']['conversions_stats']
    # Calculate correlation
    correlation = clicks_sum / conversions_sum if conversions_sum != 0 else None
    # Print analysis results
    print("Correlation between clicks and conversions:", correlation)
    print("Clicks Mean:", clicks_stats['avg'])
    print("Clicks Median:", clicks_stats['median'])
    print("Clicks Standard Deviation:", clicks_stats['std_deviation'])
    print("Conversions Mean:", conversions_stats['avg'])
    print("Conversions Median:", conversions_stats['median'])
    print("Conversions Standard Deviation:", conversions_stats['std_deviation'])

# Call the function to perform analysis
perform_analysis()

