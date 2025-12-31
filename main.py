"""
Wandern Arweave Uploader Cloud Function
Uploads finalized Geo Echoes to Arweave via Irys bundler.
"""
import functions_framework
import os
import json
import logging
from flask import Request, jsonify
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DB_CONNECTION_NAME = os.environ.get("DB_CONNECTION_NAME", "wandern-project-startup:us-central1:wandern-postgres")
DB_USER = os.environ.get("DB_USER", "wandern_app")
DB_PASS = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME", "wandern_db")

# Arweave/Irys config
ARWEAVE_WALLET_KEY = os.environ.get("ARWEAVE_WALLET_KEY")
IRYS_NODE = "https://node1.irys.xyz"


def get_db_connection():
    """Get database connection using Cloud SQL Python Connector."""
    from google.cloud.sql.connector import Connector
    import pg8000
    
    connector = Connector()
    
    def getconn():
        return connector.connect(
            DB_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
        )
    
    return getconn()


def upload_to_irys(data: dict, tags: list) -> str:
    """
    Upload data to Arweave via Irys bundler.
    Returns transaction ID.
    
    Uses Irys free tier for <100KB uploads.
    """
    import requests
    
    # For files under 100KB, Irys provides free uploads
    payload = json.dumps(data).encode('utf-8')
    
    if len(payload) > 100 * 1024:
        logger.warning(f"Payload size {len(payload)} exceeds free tier, requires funded wallet")
    
    # Irys upload endpoint
    # Note: For production, use proper Irys SDK with wallet signing
    # This is a simplified version using HTTP API
    
    headers = {
        "Content-Type": "application/json",
    }
    
    # Add wallet key if available for funded uploads
    if ARWEAVE_WALLET_KEY:
        # In production, sign the transaction with the wallet
        pass
    
    try:
        # For MVP: Use Irys HTTP upload (requires API key or funded wallet)
        # Placeholder - actual implementation needs irys-sdk
        logger.info(f"Would upload {len(payload)} bytes to Irys")
        
        # Simulate success for testing
        # TODO: Replace with actual irys-sdk upload
        fake_tx_id = f"test_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        return fake_tx_id
        
    except Exception as e:
        logger.error(f"Irys upload failed: {e}")
        raise


@functions_framework.http
def upload_batch(request: Request):
    """
    HTTP Cloud Function to batch upload Geo Echoes to Arweave.
    
    Query params:
    - priority_only: If true, only upload priority (Pro user) echoes
    
    Returns JSON with upload results.
    """
    # CORS Headers
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600"
        }
        return ("", 204, headers)

    headers = {"Access-Control-Allow-Origin": "*"}

    try:
        priority_only = request.args.get("priority_only", "false").lower() == "true"
        
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query for echoes pending Arweave upload
        query = """
            SELECT echo_id, user_id, content_text, location, created_at, is_perma_echo
            FROM geo_echoes
            WHERE is_perma_echo = TRUE
            AND arweave_tx_id IS NULL
            AND moderation_status = 'approved'
        """
        
        if priority_only:
            query += " AND is_priority = TRUE"
        
        query += " ORDER BY created_at ASC LIMIT 50"
        
        cursor.execute(query)
        echoes = cursor.fetchall()
        
        results = {
            "processed": 0,
            "uploaded": 0,
            "failed": 0,
            "tx_ids": []
        }
        
        for echo in echoes:
            echo_id, user_id, content, location, created_at, is_perma = echo
            results["processed"] += 1
            
            try:
                # Prepare Arweave data
                arweave_data = {
                    "type": "geo-echo",
                    "app": "wandern",
                    "version": "1.0",
                    "content": content,
                    "location": str(location),
                    "created_at": created_at.isoformat() if created_at else None,
                    "user_id_hash": str(hash(user_id))  # Anonymized
                }
                
                tags = [
                    {"name": "App-Name", "value": "Wandern"},
                    {"name": "Content-Type", "value": "application/json"},
                    {"name": "Type", "value": "geo-echo"},
                ]
                
                # Upload to Arweave
                tx_id = upload_to_irys(arweave_data, tags)
                
                # Update database
                cursor.execute(
                    "UPDATE geo_echoes SET arweave_tx_id = %s, arweave_uploaded_at = NOW() WHERE echo_id = %s",
                    (tx_id, echo_id)
                )
                conn.commit()
                
                results["uploaded"] += 1
                results["tx_ids"].append(tx_id)
                
            except Exception as e:
                logger.error(f"Failed to upload echo {echo_id}: {e}")
                results["failed"] += 1
        
        cursor.close()
        conn.close()
        
        return (jsonify(results), 200, headers)
        
    except Exception as e:
        logger.error(f"Batch upload failed: {e}")
        return (jsonify({"error": str(e)}), 500, headers)
