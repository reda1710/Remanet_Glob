import json
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from fastapi import WebSocket
from config import get_settings
from services.coldspray_service import get_filtered_data
from services.microphone_service import get_mic_data
from services.maintenance_service import check_predictive_maintenance

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_filters: Dict[WebSocket, Optional[str]] = {}
        self.broadcast_task = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        self.connection_filters[websocket] = None
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")
        
        # Send initial data - including mic data
        await self._send_data_to_client(websocket, None)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.connection_filters:
            del self.connection_filters[websocket]
        logger.info(f"Client disconnected. Remaining connections: {len(self.active_connections)}")

    async def update_filter(self, websocket: WebSocket, filter_date: Optional[str]):
        self.connection_filters[websocket] = filter_date
        logger.info(f"Updated filter to {filter_date}")
        
        # Send immediate filtered data
        await self._send_data_to_client(websocket, filter_date)

    async def _send_data_to_client(self, websocket: WebSocket, filter_date: Optional[str]):
        """Send data to a specific client with optional filtering"""
        try:
            # Get data for all sensors
            cold_spray_data = await get_filtered_data(filter_date)
            micro0_data = await get_mic_data("micro_0", filter_date)
            micro1_data = await get_mic_data("micro_1", filter_date)
            
            # Create combined data object
            combined_data = {
                "cold_spray": cold_spray_data["data"],
                "micro_0": micro0_data,
                "micro_1": micro1_data
            }

            # Create notifications data object
            notifications_data = {
                "notifications": cold_spray_data["notifications"]
            }

            # Check for maintenance requirements
            maintenance_required = await check_predictive_maintenance(combined_data)
            maintenance_data = {
                "maintenance_required": maintenance_required
            }

            # Send data
            await websocket.send_text(json.dumps(combined_data))
            await websocket.send_text(json.dumps(notifications_data))
            await websocket.send_text(json.dumps(maintenance_data))
        except Exception as e:
            logger.error(f"Error sending data to client: {e}")
            self.disconnect(websocket)

    async def broadcast_real_time_data(self):
        """Broadcast real-time data to clients without filters"""
        settings = get_settings()
        while True:
            try:
                # Only fetch data if there are connections
                if self.active_connections:
                    # Get real-time data for all sensors
                    cold_spray_data = await get_filtered_data(None)
                    micro0_data = await get_mic_data("micro_0", None)
                    micro1_data = await get_mic_data("micro_1", None)
                    
                    # Create combined data object
                    combined_data = {
                        "cold_spray": cold_spray_data["data"],
                        "micro_0": micro0_data,
                        "micro_1": micro1_data
                    }

                    # Create notifications data object
                    notifications_data = {
                        "notifications": cold_spray_data["notifications"]
                    }

                    # Check for maintenance requirements
                    maintenance_required = await check_predictive_maintenance(combined_data)
                    maintenance_data = {
                        "maintenance_required": maintenance_required
                    }
                    
                    # Send to each connection that doesn't have a filter
                    for websocket in self.active_connections.copy():
                        try:
                            if self.connection_filters.get(websocket) is None:
                                await websocket.send_text(json.dumps(combined_data))
                                await websocket.send_text(json.dumps(notifications_data))
                                await websocket.send_text(json.dumps(maintenance_data))
                            
                            # Send ping to all connections to keep them alive
                            await websocket.send_json({
                                "type": "ping", 
                                "timestamp": datetime.now().isoformat()
                            })
                        except Exception as e:
                            logger.error(f"Error sending to client: {e}")
                            # Use self.disconnect to prevent calling disconnect directly
                            self.disconnect(websocket)
            except Exception as e:
                logger.error(f"Error in broadcast task: {e}")
            
            # Wait before next update
            await asyncio.sleep(settings.BROADCAST_INTERVAL)

    def start_broadcast_task(self):
        """Start the broadcast task if not already running"""
        if self.broadcast_task is None or self.broadcast_task.done():
            self.broadcast_task = asyncio.create_task(self.broadcast_real_time_data())
            logger.info("Started broadcast task")
        
    def stop_broadcast_task(self):
        """Stop the broadcast task"""
        if self.broadcast_task:
            self.broadcast_task.cancel()
            logger.info("Stopped broadcast task")
