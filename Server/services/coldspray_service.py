from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any
from database import db
from data_generator import generate_sample_coldspray_data
from config import get_settings

logger = logging.getLogger(__name__)

async def get_filtered_data(filter_date: Optional[str] = None) -> Dict[str, Any]:
    """Get cold spray data with optional date filtering"""
    try:
        settings = get_settings()
        collection = db.get_collection("coldspray")
        
        if filter_date:
            # Convert string to datetime for filtering
            date_obj = datetime.strptime(filter_date, "%m/%d/%Y")
            next_day = date_obj + timedelta(days=1)
            
            # Query for specific date
            query = {
                "Time": {
                    "$gte": date_obj,
                    "$lt": next_day
                }
            }
        else:
            # For real-time data, get the last hour of data
            one_hour_ago = datetime.now() - timedelta(hours=1)
            query = {
                "Time": {
                    "$gte": one_hour_ago
                }
            }
        
        # Project only the fields we need
        projection = {
            "Time": 1,
            "T_Gun": 1, 
            "P_Gun": 1,
            "Q_PG_N2": 1,
            "V_Particule": 1,
            "Q_CG_PF1":1,
            "Q_CG_PF2":1,
            "_id": 0
        }
        
        # Try to fetch data from MongoDB
        try:
            if collection is not None:
                cursor = collection.find(query, projection).sort("Time", 1).limit(5000)
                data = list(cursor)
                logger.info(f"Retrieved {len(data)} records from coldspray")
            else:
                logger.error("Collection not available")
                data = []
        except Exception as db_error:
            logger.error(f"Database error: {db_error}")
            data = []
        
        # If no data found, generate sample data
        if not data:
            logger.info(f"No data found in database, generating sample data")
            data = generate_sample_coldspray_data()
        
        # Process data and check for notifications
        processed_data = []
        notifications = []
        
        for item in data:
            processed_item = {
                "Time": item["Time"].isoformat() if isinstance(item["Time"], datetime) else item["Time"],
                "T_gun": item["T_Gun"],
                "P_gun": item["P_Gun"],
                "Q_PG_N2": item["Q_PG_N2"],
                "V_Particule": item.get("V_Particule", 0),
                "Q_CG_PF1": item.get("Q_CG_PF1"),
                "Q_CG_PF2": item.get("Q_CG_PF2", 0)
            }
            processed_data.append(processed_item)
            
            # Generate notifications for values exceeding thresholds
            timestamp = item["Time"].isoformat() if isinstance(item["Time"], datetime) else item["Time"]
            
            if item["T_Gun"] > settings.MAX_T_GUN:
                notifications.append({
                    "type": "warning",
                    "parameter": "T_gun",
                    "value": item["T_Gun"],
                    "threshold": settings.MAX_T_GUN,
                    "timestamp": timestamp,
                    "message": f"T_Gun exceeded maximum threshold: {item['T_Gun']} > {settings.MAX_T_GUN}"
                })
                
            if item["P_Gun"] > settings.MAX_P_GUN:
                notifications.append({
                    "type": "warning",
                    "parameter": "P_gun",
                    "value": item["P_Gun"],
                    "threshold": settings.MAX_P_GUN,
                    "timestamp": timestamp,
                    "message": f"P_Gun exceeded maximum threshold: {item['P_Gun']} > {settings.MAX_P_GUN}"
                })
                
            if item["Q_PG_N2"] > settings.MAX_Q_PG_N2:
                notifications.append({
                    "type": "warning",
                    "parameter": "Q_PG_N2",
                    "value": item["Q_PG_N2"],
                    "threshold": settings.MAX_Q_PG_N2,
                    "timestamp": timestamp,
                    "message": f"Q_PG_N2 exceeded maximum threshold: {item['Q_PG_N2']} > {settings.MAX_Q_PG_N2}"
                })

            if item.get("V_Particule", 0) > settings.MAX_V_PARTICULE:
                notifications.append({
                    "type": "warning",
                    "parameter": "V_Particule",
                    "value": item.get("V_Particule", 0),
                    "threshold": settings.MAX_V_PARTICULE,
                    "timestamp": timestamp,
                    "message": f"V_Particule exceeded maximum threshold: {item.get('V_Particule', 0)} > {settings.MAX_V_PARTICULE}"
                })

        return {
            "data": processed_data,
            "notifications": notifications
        }
    except Exception as e:
        logger.error(f"Error in get_filtered_data: {e}")
        return {
            "data": [],
            "notifications": []
        }
