import logging
from typing import Dict, Any
from config import get_settings

logger = logging.getLogger(__name__)

async def check_predictive_maintenance(combined_data: Dict[str, Any]) -> bool:
    """
    Check if maintenance is required based on sensor data
    
    Args:
        combined_data: Dictionary containing all sensor data
        
    Returns:
        Boolean indicating whether maintenance is required
    """
    try:
        settings = get_settings()
        maintenance_required = False
        
        # Extract cold spray data for analysis
        cold_spray_data = combined_data.get("cold_spray", [])
        
        if not cold_spray_data:
            return False
        
        # Get the latest readings
        latest_data = cold_spray_data[-1] if cold_spray_data else None
        
        if not latest_data:
            return False
            
        # Check for concerning patterns in latest reading
        if latest_data.get("T_gun", 0) > settings.T_GUN_MAINTENANCE_THRESHOLD:
            maintenance_required = True
            logger.warning(f"T_gun exceeded maintenance threshold: {latest_data.get('T_gun', 0)}")
            
        if latest_data.get("P_gun", 0) > settings.P_GUN_MAINTENANCE_THRESHOLD:
            maintenance_required = True
            logger.warning(f"P_gun exceeded maintenance threshold: {latest_data.get('P_gun', 0)}")
            
        if latest_data.get("Q_PG_N2", 0) > settings.Q_PG_N2_MAINTENANCE_THRESHOLD:
            maintenance_required = True
            logger.warning(f"Q_PG_N2 exceeded maintenance threshold: {latest_data.get('Q_PG_N2', 0)}")
        
        if latest_data.get("V_Particule", 0) > settings.V_PARTICULE_MAINTENANCE_THRESHOLD:
            maintenance_required = True
            logger.warning(f"V_Particule exceeded maintenance threshold: {latest_data.get('V_Particule', 0)}")
            
        return maintenance_required
        
    except Exception as e:
        logger.error(f"Error in predictive maintenance check: {e}")
        return False
