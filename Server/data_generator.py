import random
from datetime import datetime, timedelta
import numpy as np
import base64

def generate_sample_coldspray_data(num_points=50):
    """Generate sample cold spray data for testing"""
    start_time = datetime.now() - timedelta(seconds=num_points)
    data = []
    
    for i in range(num_points):
        time = start_time + timedelta(seconds=i)
        data.append({
            "Time": time,
            "T_Gun": 45 + random.uniform(-5, 5) + (i % 10),  # Temperature pattern
            "P_Gun": 45 + random.uniform(-3, 3) + (i % 5),  # Pressure pattern
            "Q_PG_N2": 45 + random.uniform(-2, 2) + (i % 7),  # Flow rate pattern
            "V_Particule": 45 + random.uniform(-2, 2) + (i % 7),  # V_Particule pattern
            "Q_CG_PF1": 45 + random.uniform(-1, 1) + (i % 3),  # Feeder 1 Flow rate pattern
            "Q_CG_PF2": 45 + random.uniform(-1, 1) + (i % 3)   # Feeder 2 Flow rate pattern
        })
    
    return data

def generate_mic_waveform(num_samples=100):
    """Generate sample microphone waveform data"""
    t = np.linspace(0, 1, num_samples)
    
    # Generate base sine wave with some noise
    frequency = random.uniform(10, 20)  # Random frequency
    amplitude = random.uniform(0.5, 1.0)  # Random amplitude
    
    # Create waveform with harmonics
    waveform = amplitude * np.sin(2 * np.pi * frequency * t)
    waveform += 0.3 * np.sin(2 * np.pi * frequency * 2 * t)  # Add first harmonic
    waveform += 0.15 * np.sin(2 * np.pi * frequency * 3 * t)  # Add second harmonic
    
    # Add some noise
    noise = np.random.normal(0, 0.05, num_samples)
    waveform += noise
    
    # Convert to float32 (typical audio format)
    waveform = waveform.astype(np.float32)
    
    # Convert to base64 for transmission
    waveform_bytes = waveform.tobytes()
    base64_data = base64.b64encode(waveform_bytes).decode('utf-8')
    
    return {
        "timestamp": datetime.now(),    
        "data": base64_data,
    }
