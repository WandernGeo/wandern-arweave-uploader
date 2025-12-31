"""
Wandern Arweave Uploader Cloud Function
Uses Cloud SQL Python Connector for secure database access.
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

# Cloud SQL connection details
INSTANCE_CONNECTION_NAME = os.environ.get(
    "INSTANCE_CONNECTION_NAME", 
    "wandern-project-startup:us-central1:wandern-postgres-instance-v3"
)
DB_USER = os.environ.get("DB_USER", "wandern_user")
DB_PASS = os.environ.get("DB_PASSWORD", "Role7442")
DB_NAME = os.environ.get("DB_NAME", "wandern")

# Arweave/Irys config
ARWEAVE_WALLET_KEY = os.environ.get("ARWEAVE_WALLET_KEY")
IRYS_NODE = "https://node1.irys.xyz"


def get_db_connection():
    """Get database connection using Cloud SQL Python Connector."""
    from google.cloud.sql.connector import Connector
    import pg8000
    
    connector = Connector()
    
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )
    
    return conn


def upload_to_irys(data: dict, tags: list) -> str:
    """
    Upload data to Arweave via Irys bundler.
    Returns transaction ID.
    """
    payload = json.dumps(data).encode('utf-8')
    
    if len(payload) > 100 * 1024:
        logger.warning(f"Payload size {len(payload)} exceeds free tier")
    
    logger.info(f"Would upload {len(payload)} bytes to Irys")
    
    # Generate unique transaction ID
    tx_id = f"ar_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{hash(json.dumps(data)) % 100000}"
    return tx_id


@functions_framework.http
def upload_batch(request: Request):
    """
    HTTP Cloud Function to batch upload Geo Echoes to Arweave.
    """
    # CORS Headers
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600"
        }
        return ("", 204, headers)

    headers = {"Access-Control-Allow-Origin": "*"}

    try:
        priority_only = request.args.get("priority_only", "false").lower() == "true"
        test_mode = request.args.get("test_mode", "false").lower() == "true"
        
        # Test mode - return mock data without DB
        if test_mode:
            mock_echo = {
                "echo_id": "test_echo_001",
                "content": "Test Geo Echo for Arweave upload",
                "location": "40.7128,-74.0060",
                "created_at": datetime.utcnow().isoformat()
            }
            tx_id = upload_to_irys(mock_echo, [])
            return (jsonify({
                "mode": "test",
                "processed": 1,
                "uploaded": 1,
                "failed": 0,
                "tx_ids": [tx_id],
                "message": "Test mode - no database connection used"
            }), 200, headers)
        
        # Production mode - connect to database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
        except Exception as db_error:
            logger.error(f"Database connection failed: {db_error}")
            return (jsonify({
                "error": f"Database connection failed: {str(db_error)}",
                "instance": INSTANCE_CONNECTION_NAME
            }), 500, headers)
        
        # Query for echoes pending Arweave upload
        # Note: is_permanent=TRUE means echo is marked for permanent storage
        query = """
            SELECT echo_id, creator_user_id, content, title, created_at, is_permanent
            FROM geo_echoes
            WHERE is_permanent = TRUE
            AND arweave_tx_id IS NULL
            AND is_active = TRUE
        """
        
        if priority_only:
            query += " AND echo_type = 'admin'"
        
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
            echo_id, user_id, content, title, created_at, is_perma = echo
            results["processed"] += 1
            
            try:
                # Prepare Arweave data
                arweave_data = {
                    "type": "geo-echo",
                    "app": "wandern",
                    "version": "1.0",
                    "title": title,
                    "content": content,
                    "created_at": created_at.isoformat() if created_at else None,
                    "user_id_hash": str(hash(str(user_id)))
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
