"""
Auth API Router

Endpoints for user authentication and API key management.
"""
from datetime import datetime, timedelta
from typing import Optional, List
import json

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr

from auth import (
    get_current_org, OrgContext, 
    authenticate_user, create_jwt_token, 
    create_api_key_in_db, hash_password
)
from database import execute_query, execute_single, execute_insert

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])


# ============================================
# Request/Response Models
# ============================================

class LoginRequest(BaseModel):
    """Login request payload"""
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    """Login response with JWT"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    user: dict


class SetPasswordRequest(BaseModel):
    """Set password for invited user"""
    invite_token: str
    password: str
    name: Optional[str] = None


class CreateAPIKeyRequest(BaseModel):
    """Request to create a new API key"""
    name: str
    scopes: List[str] = ["ingest"]
    expires_in_days: Optional[int] = None


class APIKeyResponse(BaseModel):
    """Response with new API key (shown once)"""
    id: str
    key: str  # Full key, shown only once
    key_prefix: str
    name: str
    scopes: List[str]
    expires_at: Optional[datetime] = None
    warning: str = "Save this key - it cannot be retrieved again"


class APIKeyInfo(BaseModel):
    """API key info (without the actual key)"""
    id: str
    key_prefix: str
    name: str
    scopes: List[str]
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    created_at: datetime


# ============================================
# Auth Endpoints
# ============================================

@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """
    Authenticate user and return JWT token
    
    **Example:**
    ```bash
    curl -X POST https://api.example.com/api/v1/auth/login \\
      -H "Content-Type: application/json" \\
      -d '{"email": "user@example.com", "password": "secret"}'
    ```
    """
    user = authenticate_user(request.email, request.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Create JWT token
    token = create_jwt_token(
        user_id=str(user["id"]),
        org_id=str(user["organization_id"]),
        email=user["email"],
        role=user["role"]
    )
    
    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_in=24 * 60 * 60,  # 24 hours in seconds
        user={
            "id": str(user["id"]),
            "email": user["email"],
            "name": user.get("name"),
            "role": user["role"],
            "org_id": str(user["organization_id"])
        }
    )


class RegisterRequest(BaseModel):
    """Registration request payload"""
    email: EmailStr
    password: str
    name: str


@router.post("/register", response_model=LoginResponse)
async def register(request: RegisterRequest):
    """
    Register a new user (open registration)
    
    Creates a new user and organization, then returns JWT token.
    
    **Example:**
    ```bash
    curl -X POST https://api.example.com/api/v1/auth/register \\
      -H "Content-Type: application/json" \\
      -d '{"email": "user@example.com", "password": "secret123", "name": "John Doe"}'
    ```
    """
    # Check if email already exists
    existing = execute_single("""
        SELECT id FROM users WHERE email = %s
    """, (request.email,))
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create organization for new user
    org = execute_single("""
        INSERT INTO organizations (name) 
        VALUES (%s) 
        RETURNING id, name
    """, (f"{request.name}'s Organization",), commit=True)
    
    if not org:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create organization"
        )
    
    # Hash password and create user
    password_hash = hash_password(request.password)
    
    user = execute_single("""
        INSERT INTO users (organization_id, email, password_hash, name, role, email_verified)
        VALUES (%s, %s, %s, %s, 'admin', TRUE)
        RETURNING id, organization_id, email, name, role
    """, (str(org["id"]), request.email, password_hash, request.name), commit=True)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )
    
    # Create JWT token
    token = create_jwt_token(
        user_id=str(user["id"]),
        org_id=str(user["organization_id"]),
        email=user["email"],
        role=user["role"]
    )
    
    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_in=24 * 60 * 60,
        user={
            "id": str(user["id"]),
            "email": user["email"],
            "name": user["name"],
            "role": user["role"],
            "org_id": str(user["organization_id"])
        }
    )


@router.post("/set-password", response_model=LoginResponse)
async def set_password(request: SetPasswordRequest):
    """
    Set password for an invited user
    
    Users receive an invite token via email, then set their password here.
    """
    # Find user by invite token
    user = execute_single("""
        SELECT id, organization_id, email, name, role, invite_expires_at
        FROM users
        WHERE invite_token = %s AND password_hash IS NULL
    """, (request.invite_token,))
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired invite token"
        )
    
    # Check expiry
    if user.get("invite_expires_at") and user["invite_expires_at"] < datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invite token has expired"
        )
    
    # Hash and save password
    password_hash = hash_password(request.password)
    name = request.name or user.get("name")
    
    execute_insert("""
        UPDATE users 
        SET password_hash = %s, name = %s, invite_token = NULL, email_verified = TRUE, updated_at = NOW()
        WHERE id = %s
    """, (password_hash, name, str(user["id"])))
    
    # Create JWT token
    token = create_jwt_token(
        user_id=str(user["id"]),
        org_id=str(user["organization_id"]),
        email=user["email"],
        role=user["role"]
    )
    
    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_in=24 * 60 * 60,
        user={
            "id": str(user["id"]),
            "email": user["email"],
            "name": name,
            "role": user["role"],
            "org_id": str(user["organization_id"])
        }
    )


# ============================================
# API Key Management Endpoints
# ============================================

@router.post("/api-keys", response_model=APIKeyResponse)
async def create_api_key(
    request: CreateAPIKeyRequest,
    org: OrgContext = Depends(get_current_org)
):
    """
    Create a new API key for Spark jobs
    
    **Authentication:** Requires JWT (user must be logged in).
    
    **Warning:** The key is only shown once in the response.
    
    **Example:**
    ```bash
    curl -X POST https://api.example.com/api/v1/auth/api-keys \\
      -H "Authorization: Bearer <jwt>" \\
      -H "Content-Type: application/json" \\
      -d '{"name": "Production ETL", "scopes": ["ingest"]}'
    ```
    """
    # Require JWT auth (not API key) for creating new keys
    if org.source != "jwt":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API keys can only be created by logged-in users, not other API keys"
        )
    
    # Create the key
    raw_key, key_record = create_api_key_in_db(
        org_id=org.org_id,
        name=request.name,
        scopes=request.scopes,
        created_by=org.user_id,
        expires_in_days=request.expires_in_days
    )
    
    scopes = key_record.get("scopes", request.scopes)
    if isinstance(scopes, str):
        scopes = json.loads(scopes)
    
    return APIKeyResponse(
        id=str(key_record["id"]),
        key=raw_key,
        key_prefix=key_record["key_prefix"],
        name=key_record["name"],
        scopes=scopes,
        expires_at=key_record.get("expires_at")
    )


@router.get("/api-keys", response_model=List[APIKeyInfo])
async def list_api_keys(
    org: OrgContext = Depends(get_current_org)
):
    """
    List all API keys for the organization
    
    **Note:** The actual key values are never returned after creation.
    """
    keys = execute_query("""
        SELECT id, key_prefix, name, scopes, last_used_at, expires_at, created_at
        FROM api_keys
        WHERE organization_id = %s AND revoked = FALSE
        ORDER BY created_at DESC
    """, (org.org_id,))
    
    result = []
    for key in keys:
        scopes = key.get("scopes", ["ingest"])
        if isinstance(scopes, str):
            scopes = json.loads(scopes)
        
        result.append(APIKeyInfo(
            id=str(key["id"]),
            key_prefix=key["key_prefix"],
            name=key["name"],
            scopes=scopes,
            last_used_at=key.get("last_used_at"),
            expires_at=key.get("expires_at"),
            created_at=key["created_at"]
        ))
    
    return result


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(
    key_id: str,
    org: OrgContext = Depends(get_current_org)
):
    """
    Revoke an API key
    
    The key will immediately stop working for all requests.
    """
    # Verify key belongs to this org
    key = execute_single("""
        SELECT id FROM api_keys WHERE id = %s AND organization_id = %s
    """, (key_id, org.org_id))
    
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    execute_insert("""
        UPDATE api_keys SET revoked = TRUE, revoked_at = NOW() WHERE id = %s
    """, (key_id,))
    
    return {"status": "revoked", "key_id": key_id}


# ============================================
# Token Validation Endpoint
# ============================================

@router.get("/me")
async def get_current_user(
    org: OrgContext = Depends(get_current_org)
):
    """
    Get current user/key info
    
    Useful for verifying tokens and getting org context.
    """
    # Get org name
    org_info = execute_single("""
        SELECT id, name FROM organizations WHERE id = %s
    """, (org.org_id,))
    
    result = {
        "authenticated": True,
        "source": org.source,
        "org_id": org.org_id,
        "org_name": org_info.get("name") if org_info else None,
        "scopes": org.scopes
    }
    
    if org.user_id:
        user = execute_single("""
            SELECT id, email, name, role FROM users WHERE id = %s
        """, (org.user_id,))
        if user:
            result["user"] = {
                "id": str(user["id"]),
                "email": user["email"],
                "name": user.get("name"),
                "role": user["role"]
            }
    
    return result
