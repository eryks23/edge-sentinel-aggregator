import time
import random
import hashlib
import json
from collections import deque
from datetime import datetime
from typing import Dict, List, Any

class EdgeDataAggregator:
    def __init__(self, node_id: str, batch_size: int = 5):
        self.node_id = node_id
        self.batch_size = batch_size
        self.pending_batch: List[Dict[str, Any]] = []
        self.critical_threshold = 40.0
        self.timeout = 10
        self.last_flush_time = time.time()

    def generate_checksum(self, data: str) -> str:
        return hashlib.sha256(data.encode()).hexdigest()

    def get_reading(self) -> float:
        return round(random.uniform(20.0, 50.0), 3)

    def process_cycle(self) -> None:
        
        temp = self.get_reading()
        
        if temp > self.critical_threshold:
            entry = {

                "t": temp,
                "ts": time.time()

            }

            self.pending_batch.append(entry)
            print(f"Alert added to batch. Current size: {len(self.pending_batch)}")

        time_since_last = time.time() - self.last_flush_time

        if len(self.pending_batch) >= self.batch_size:
            self.flush_batch()
            return
            
        elif self.pending_batch and time_since_last > self.timeout:
            print(f"Timeout ({round(time_since_last, 1)}s) - Forced flush")
            self.flush_batch()
            return
        
        if temp <= self.critical_threshold:
            print(f"DEBUG: {temp}°C - Normal")

    def flush_batch(self) -> None:

        if not self.pending_batch:
            return
        
        json_str = json.dumps(self.pending_batch)
        check = self.generate_checksum(json_str)
        
        envelope = {

            "node_id": self.node_id,
            "data": self.pending_batch,
            "checksum": check

        }

        try:
            self.send_to_cloud(envelope)
        except Exception as e:
            print(f"Error during transmission: {e}")
        finally:
            self.pending_batch.clear()
            self.last_flush_time = time.time()
        
        print(f"[SYSTEM] Preparing batch for transmission...")

    def send_to_cloud(self, envelope: Dict[str, Any]) -> None:
        print(f"\n[CLOUD TRANSMISSION]")
        print(f"Node ID: {envelope.get('node_id')}")
        print(f"Checksum: {envelope.get('checksum')}")
        print(f"Payload Size: {len(json.dumps(envelope['data']))} bytes")
        print(f"Items in batch: {len(envelope['data'])}")
        print("------------------------------------------\n")

def main():
    aggregator = EdgeDataAggregator(node_id="EDGE-SECURE-001", batch_size=3)
    print(f"Aggregator Started: {aggregator.node_id} (Batch Size: {aggregator.batch_size})")
    
    try:
        while True:
            aggregator.process_cycle()
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping aggregator...")

if __name__ == "__main__":
    main()
