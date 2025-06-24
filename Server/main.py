import json
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from config import get_settings, Settings
from database import db
from websocket_manager import ConnectionManager
from services.coldspray_service import get_filtered_data

# Set up proper logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Remanet Data Ingestion Service")

# Add CORS middleware with specific origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create connection manager instance
manager = ConnectionManager()

# REST endpoint for testing
@app.get("/data/")
async def get_data(
    filter_date: Optional[str] = Query(None),
    settings: Settings = Depends(get_settings)
):
    return await get_filtered_data(filter_date)

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    
    # Start broadcast task
    manager.start_broadcast_task()
    
    try:
        while True:
            # Wait for messages from the client
            data = await websocket.receive_text()
            logger.info(f"Received message: {data[:100]}")
            
            try:
                message = json.loads(data)
                
                # Update filter if received
                if "filter_date" in message:
                    await manager.update_filter(websocket, message["filter_date"])
                
                # Handle ping messages
                if message.get("type") == "ping":
                    await websocket.send_json({
                        "type": "pong",
                        "timestamp": datetime.now().isoformat()
                    })
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON received")
    
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "connections": len(manager.active_connections),
        "mongodb": "connected" if db.db is not None else "disconnected"
    }

# Startup event
@app.on_event("startup")
async def startup_event():
    # Initialize database connection
    db.initialize()
    # Start the broadcast task
    manager.start_broadcast_task()
    logger.info("FastAPI application started")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    # Stop the broadcast task
    manager.stop_broadcast_task()
    # Close database connection
    db.close()
    logger.info("FastAPI application shutdown")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
