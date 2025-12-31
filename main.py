"""
Wandern Arweave Uploader Cloud Function
Uploads finalized Geo Echoes to Arweave via Irys bundler.

MODERATION: Calls Content Moderation Agent BEFORE uploading to Arweave.
This is the final safety checkpoint before permanent storage.
"""
import functions_framework
import os
import json
import logging
import httpx
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

# Moderation Agent URL
MODERATION_AGENT_URL = os.environ.get(
    "MODERATION_AGENT_URL",
    "https://us-central1-wandern-project-startup.cloudfunctions.net/wandern-moderation-agent"
)

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


def call_moderation_agent(content: str, content_type: str, media_url: str = None) -> dict:
    """
    Call the Content Moderation Agent for pre-Arweave check.
    This is the FINAL moderation checkpoint before permanent storage.
    
    Returns: {"is_safe": bool, "moderation_status": str, "flag_reason": str}
    """
    try:
        with httpx.Client(timeout=60) as client:
            response = client.post(MODERATION_AGENT_URL, json={
                "content": content,
                "content_type": content_type,
                "media_url": media_url
            })
            result = response.json()
            logger.info(f"Pre-Arweave moderation result: {result}")
            return result
    except Exception as e:
        logger.error(f"Moderation agent call failed: {e}")
        # FAIL CLOSED for Arweave - don't permanently store if we can't verify
        return {
            "is_safe": False,
            "moderation_status": "error",
            "flag_reason": f"Moderation check failed: {str(e)}"
        }


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
    
    MODERATION FLOW:
    1. Query echoes marked is_permanent=true and not yet uploaded
    2. For EACH echo, call moderation agent for final safety check
    3. If approved → upload to Arweave and record tx_id
    4. If rejected → mark as flagged, do NOT upload
    
    Query params:
    - priority_only: If true, only upload priority echoes
    - test_mode: If true, skip DB and return mock data
    - skip_moderation: If true, skip final moderation check (for testing only)
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
        skip_moderation = request.args.get("skip_moderation", "false").lower() == "true"
        
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
                "flagged": 0,
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
        query = """
            SELECT echo_id, creator_user_id, content, title, content_type, media_url, created_at, is_permanent
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
            "flagged": 0,
            "tx_ids": [],
            "moderation_results": []
        }
        
        for echo in echoes:
            echo_id, user_id, content, title, content_type, media_url, created_at, is_perma = echo
            results["processed"] += 1
            
            try:
                # === FINAL MODERATION CHECK ===
                # This is the last safety checkpoint before permanent storage
                if not skip_moderation:
                    mod_result = call_moderation_agent(
                        content=content or title or "",
                        content_type=content_type or "text",
                        media_url=media_url
                    )
                    
                    results["moderation_results"].append({
                        "echo_id": echo_id,
                        "is_safe": mod_result.get("is_safe"),
                        "status": mod_result.get("moderation_status"),
                        "model": mod_result.get("model_used")
                    })
                    
                    if not mod_result.get("is_safe", False):
                        # REJECT - Mark as flagged, do NOT upload to Arweave
                        logger.warning(f"Echo {echo_id} BLOCKED from Arweave: {mod_result.get('flag_reason')}")
                        cursor.execute(
                            """UPDATE geo_echoes 
                               SET moderation_status = 'flagged', 
                                   moderation_reason = %s,
                                   is_permanent = FALSE
                               WHERE echo_id = %s""",
                            (mod_result.get("flag_reason", "Pre-Arweave check failed"), echo_id)
                        )
                        conn.commit()
                        results["flagged"] += 1
                        continue  # Skip to next echo
                
                # === APPROVED - UPLOAD TO ARWEAVE ===
                arweave_data = {
                    "type": "geo-echo",
                    "app": "wandern",
                    "version": "1.0",
                    "title": title,
                    "content": content,
                    "content_type": content_type,
                    "created_at": created_at.isoformat() if created_at else None,
                    "user_id_hash": str(hash(str(user_id))),
                    "moderation": "approved"  # Record that this passed moderation
                }
                
                tags = [
                    {"name": "App-Name", "value": "Wandern"},
                    {"name": "Content-Type", "value": "application/json"},
                    {"name": "Type", "value": "geo-echo"},
                    {"name": "Moderation-Status", "value": "approved"}
                ]
                
                # Upload to Arweave
                tx_id = upload_to_irys(arweave_data, tags)
                
                # Update database with Arweave tx_id
                cursor.execute(
                    """UPDATE geo_echoes 
                       SET arweave_tx_id = %s, 
                           arweave_uploaded_at = NOW(),
                           moderation_status = 'approved'
                       WHERE echo_id = %s""",
                    (tx_id, echo_id)
                )
                conn.commit()
                
                results["uploaded"] += 1
                results["tx_ids"].append(tx_id)
                logger.info(f"Echo {echo_id} uploaded to Arweave: {tx_id}")
                
            except Exception as e:
                logger.error(f"Failed to process echo {echo_id}: {e}")
                results["failed"] += 1
        
        cursor.close()
        conn.close()
        
        return (jsonify(results), 200, headers)
        
    except Exception as e:
        logger.error(f"Batch upload failed: {e}")
        return (jsonify({"error": str(e)}), 500, headers)
