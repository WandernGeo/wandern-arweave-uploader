"""
Wandern Arweave Uploader Cloud Function
Uploads finalized Geo Echoes to Arweave via Irys bundler.

NOTE: For production, provide DB_PASSWORD via environment variable or Secret Manager.
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
DB_HOST = os.environ.get("DB_HOST", "/cloudsql/wandern-project-startup:us-central1:wandern-postgres")
DB_USER = os.environ.get("DB_USER", "wandern_app")
DB_PASS = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "wandern_db")
USE_CLOUDSQL = os.environ.get("USE_CLOUDSQL", "false").lower() == "true"

# Arweave/Irys config
ARWEAVE_WALLET_KEY = os.environ.get("ARWEAVE_WALLET_KEY")
IRYS_NODE = "https://node1.irys.xyz"


def get_db_connection():
    """Get database connection - supports both Cloud SQL and direct connection."""
    import psycopg2
    
    if USE_CLOUDSQL:
        # Use Unix socket for Cloud SQL (deployed environment)
        return psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
    else:
        # Direct connection for testing
        db_url = os.environ.get("DATABASE_URL")
        if db_url:
            return psycopg2.connect(db_url)
        else:
            # Fallback - won't work without proper config
            raise Exception("No database configuration provided. Set DATABASE_URL or USE_CLOUDSQL=true with credentials.")


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
    
    # For MVP: Simulate upload and return test ID
    # TODO: Implement actual Irys SDK upload with wallet signing
    logger.info(f"Would upload {len(payload)} bytes to Irys")
    
    # Generate unique transaction ID for testing
    tx_id = f"ar_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{hash(json.dumps(data)) % 100000}"
    return tx_id


@functions_framework.http
def upload_batch(request: Request):
    """
    HTTP Cloud Function to batch upload Geo Echoes to Arweave.
    
    Query params:
    - priority_only: If true, only upload priority (Pro user) echoes
    - test_mode: If true, simulate DB and return mock data
    
    Returns JSON with upload results.
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
                "hint": "Set DATABASE_URL or configure Cloud SQL credentials"
            }), 500, headers)
        
        # Query for echoes pending Arweave upload
        query = """
            SELECT echo_id, user_id, content_text, ST_AsText(location) as location, created_at, is_perma_echo
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
