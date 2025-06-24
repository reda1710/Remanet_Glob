from datetime import datetime, timedelta
import logging
import base64
from typing import Optional, List, Dict, Any
from bson.binary import Binary
from database import db
from data_generator import generate_mic_waveform

logger = logging.getLogger(__name__)

async def get_mic_data(collection_name: str, filter_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get microphone data with optional date filtering"""
    try:
        # Validate collection name
        if collection_name not in ["micro_0", "micro_1"]:
            logger.error(f"Invalid collection name: {collection_name}")
            return []
            
        mic_collection = db.get_collection(collection_name)
        
        if filter_date:
            # Convert string to datetime for filtering
            date_obj = datetime.strptime(filter_date, "%m/%d/%Y")
            next_day = date_obj + timedelta(days=1)
            
            # Query for specific date
            query = {
                "timestamp": {
                    "$gte": date_obj,
                    "$lt": next_day
                }
            }
        else:
            # For real-time data, get the last hour of data
            one_hour_ago = datetime.now() - timedelta(hours=1)
            query = {
                "timestamp": {
                    "$gte": one_hour_ago
                }
            }
        
        # Project only the fields we need
        projection = {
            "timestamp": 1,
            "data": 1,
            "_id": 0
        }
        
        # Try to fetch data from MongoDB
        try:
            if mic_collection is not None:  # Changed from if mic_collection:
                cursor = mic_collection.find(query, projection).sort("timestamp", 1).limit(10)
                data = list(cursor)
                logger.info(f"Retrieved {len(data)} records from {collection_name}")
            else:
                logger.error(f"Collection {collection_name} not available")
                data = []
        except Exception as db_error:
            logger.error(f"Database error when fetching mic data: {db_error}")
            data = []
        
        # If no data found, generate sample data
        if not data:
            logger.info(f"No data found in {collection_name}, generating sample data")
            # Generate a single sample of mic data
            sample = generate_mic_waveform()
            sample["mic_id"] = collection_name
            data = [sample]
        
        # Process data to be JSON serializable
        processed_data = []
        for item in data:
            # Handle data that might be binary or already processed
            if isinstance(item.get("data"), Binary):
                base64_data = base64.b64encode(item["data"]).decode('utf-8')
            else:
                base64_data = item.get("data", "")
                
            processed_item = {
                "timestamp": item["timestamp"].isoformat() if isinstance(item["timestamp"], datetime) else item["timestamp"],
                "data": base64_data,
                "mic_id": collection_name
            }
            processed_data.append(processed_item)
            
        return processed_data
       
    except Exception as e:
        logger.error(f"Error in get_mic_data: {e}")
        return []
