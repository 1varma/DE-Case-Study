import asyncio
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import avro.schema
import avro.io
import io
import win32api
import re

# Kafka configuration
bootstrap_servers = 'localhost:9092'
ad_impressions_topic = 'ad_impressions'
clicks_conversions_topic = 'clicks_conversions'
avro_data_topic = 'avro_data_topic'
timestamp_regex = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
website_regex = r'https?://(?:www\.)?\w+\.\w+(?:/\S*)?'
# Elasticsearch configuration
es = Elasticsearch("http://localhost:9200")

# Avro schema
schema = avro.schema.Parse(open("bid_request_schema.avsc", "rb").read())

# Create Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    group_id='ad_processing_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
)

consumer.subscribe(topics=[ad_impressions_topic, clicks_conversions_topic, avro_data_topic])

# Function to validate ad impressions
async def validate_ad_impression(ad_impression):
    # Example validation: check if ad impression data is not empty
    if not ad_impression:
        win32api.MessageBox("No ad impression data present")
        return False
    if not bool(re.search(timestamp_regex, ad_impression['timestamp'])):
        win32api.MessageBox("Date not correct")
        return False

    if not bool(re.search(website_regex, ad_impression['website'])):
        win32api.MessageBox("Website Data not correct")
        return False

    return True

# Function to validate clicks/conversions
async def validate_click_conversion(click_conversion):
    # Example validation: check if click/conversion data is not empty
    if not click_conversion:
        win32api.MessageBox("No click conversion data present")
        return False

    if not bool(re.search(timestamp_regex, click_conversion['timestamp'])):
        win32api.MessageBox("Date not correct")
        return False

    if click_conversion['conversion'] in ['Yes', 'No']:
        win32api.MessageBox("Conversion has garbage values")
        return False

    return True

# Function to validate Avro data
async def validate_avro_data(avro_data):
    # Example validation: check if Avro data is not empty
    if not avro_data:
        win32api.MessageBox("No avro data present")
        return False
    return True

# Function to filter ad impressions
async def filter_ad_impression(ad_impression):
    # Example filtering: filter out invalid ad impressions
    if ad_impression['user_id'] < 0:
        return False
    return True

# Function to filter clicks/conversions
async def filter_click_conversion(click_conversion):
    # Example filtering: filter out invalid clicks/conversions
    if click_conversion['user_id'] < 0:
        return False
    return True

# Function to deduplicate data using Elasticsearch
async def deduplicate_data(ad_impression, click_conversion):
    # Your deduplication logic here
    pass

# Function to process Avro data
async def process_avro_data(avro_data):
    # Your Avro data processing logic here
    pass

# Function to write data to Elasticsearch
async def write_to_elasticsearch(data, index_name):
    # Your Elasticsearch writing logic here
    pass

# Main function to handle Kafka messages
async def handle_messages():
    async for message in consumer:
        if message.topic == ad_impressions_topic:
            ad_impression = message.value
            # Validate and filter ad impression
            if await validate_ad_impression(ad_impression) and await filter_ad_impression(ad_impression):
                # Deduplicate ad impression data
                deduplicated_data = await deduplicate_data(ad_impression, None)
                if deduplicated_data:
                    # Write deduplicated ad impression to Elasticsearch
                    await write_to_elasticsearch(deduplicated_data, 'ad_impressions_index')
        elif message.topic == clicks_conversions_topic:
            click_conversion = message.value
            # Validate and filter click/conversion
            if await validate_click_conversion(click_conversion) and await filter_click_conversion(click_conversion):
                # Deduplicate click/conversion data
                deduplicated_data = await deduplicate_data(None, click_conversion)
                if deduplicated_data:
                    # Write deduplicated click/conversion to Elasticsearch
                    await write_to_elasticsearch(deduplicated_data, 'clicks_conversions_index')
        elif message.topic == avro_data_topic:
            avro_data = message.value
            # Validate Avro data
            if await validate_avro_data(avro_data):
                # Process Avro data
                processed_avro_data = await process_avro_data(avro_data)
                # Write processed Avro data to Elasticsearch
                await write_to_elasticsearch(processed_avro_data, 'avro_data_index')

# Run the event loop
asyncio.run(handle_messages())

