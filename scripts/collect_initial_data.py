#!/usr/bin/env python
"""Collect initial weather and tourism data for Weather Flick"""

import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

import logging
from app.core.database_manager import SyncDatabaseManager
from app.collectors.weather_api_collector import WeatherAPICollector
from app.collectors.kto_api import KTODataCollector
from app.core.multi_api_key_manager import MultiAPIKeyManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def collect_weather_data():
    """Collect weather data for major regions"""
    try:
        db_manager = SyncDatabaseManager()
        key_manager = MultiAPIKeyManager()
        
        # Get regions
        regions_query = """
        SELECT region_code, region_name, latitude, longitude
        FROM regions
        WHERE region_level = 1
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        LIMIT 10
        """
        
        regions = db_manager.execute_query(regions_query)
        logger.info(f"Found {len(regions)} regions to collect weather data")
        
        # Collect weather data for each region
        weather_collector = WeatherAPICollector(db_manager)
        
        for region in regions:
            try:
                logger.info(f"Collecting weather for {region['region_name']}")
                # Note: WeatherAPICollector would need to be implemented
                # This is a placeholder for the actual collection logic
                
            except Exception as e:
                logger.error(f"Error collecting weather for {region['region_name']}: {e}")
                
    except Exception as e:
        logger.error(f"Error in weather collection: {e}")
        
def collect_tourism_data():
    """Collect tourism data from Korea Tourism Organization"""
    try:
        db_manager = SyncDatabaseManager()
        kto_client = KTODataCollector()
        
        # Get regions
        regions_query = """
        SELECT region_code, region_name
        FROM regions
        WHERE region_level = 1
        LIMIT 5
        """
        
        regions = db_manager.execute_query(regions_query)
        logger.info(f"Found {len(regions)} regions to collect tourism data")
        
        # Collect tourist attractions for each region
        for region in regions:
            try:
                logger.info(f"Collecting attractions for {region['region_name']}")
                
                # Search for tourist attractions
                attractions = kto_client.get_area_based_list(
                    area_code=region['region_code'],
                    content_type_id='12',  # Tourist attractions
                    num_of_rows=10
                )
                
                if attractions and 'items' in attractions:
                    items = attractions['items'].get('item', [])
                    if not isinstance(items, list):
                        items = [items]
                    
                    for item in items:
                        # Insert into database
                        insert_query = """
                        INSERT INTO tourist_attractions (
                            content_id, title, address, 
                            latitude, longitude, 
                            region_code, image_url,
                            description
                        ) VALUES (
                            %(content_id)s, %(title)s, %(address)s,
                            %(latitude)s, %(longitude)s,
                            %(region_code)s, %(image_url)s,
                            %(description)s
                        ) ON CONFLICT (content_id) DO NOTHING
                        """
                        
                        db_manager.execute_query(insert_query, {
                            'content_id': item.get('contentid'),
                            'title': item.get('title'),
                            'address': item.get('addr1'),
                            'latitude': float(item.get('mapy', 0)),
                            'longitude': float(item.get('mapx', 0)),
                            'region_code': region['region_code'],
                            'image_url': item.get('firstimage'),
                            'description': item.get('overview', '')[:500]
                        })
                    
                    logger.info(f"Inserted {len(items)} attractions for {region['region_name']}")
                    
            except Exception as e:
                logger.error(f"Error collecting attractions for {region['region_name']}: {e}")
                
    except Exception as e:
        logger.error(f"Error in tourism collection: {e}")

def main():
    """Main function to collect initial data"""
    logger.info("Starting initial data collection...")
    
    # Collect tourism data first (more important)
    logger.info("Collecting tourism data...")
    collect_tourism_data()
    
    # Then collect weather data
    logger.info("Collecting weather data...")
    collect_weather_data()
    
    logger.info("Initial data collection completed!")

if __name__ == "__main__":
    main()