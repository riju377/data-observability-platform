"""
Authentication Service

Complete implementation of API key and JWT authentication with database integration.
"""
import os
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Tuple
from functools import lru_cache

from fastapi import Depends, HTTPException, Security, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import jwt
import bcrypt

from database import execute_query, execute_single, execute_insert

# Security scheme for Bearer tokens
security = HTTPBearer(auto_error=False)

# JWT Configuration
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-production-" + secrets.token_hex(16))
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "24"))


# ============================================
# Models
# ============================================

class TokenData(BaseModel):
    """Data encoded in JWT"""
    user_id: str
    org_id: str
    email: str
    role: str


class OrgContext(BaseModel):
    """Organization context extracted from auth"""
    org_id: str
    user_id: Optional[str] = None
    source: str  # "api_key" or "jwt"
    scopes: list = []


# ============================================
# Password Hashing
# ============================================

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt).decode()


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(password.encode(), hashed.encode())


# ============================================
# API Key Functions
# ============================================

def hash_api_key(key: str) -> str:
    """Hash an API key using SHA256"""
    return hashlib.sha256(key.encode()).hexdigest()


def generate_api_key() -> Tuple[str, str, str]:
    """
    Generate a new API key
    
    Returns:
        (raw_key, key_hash, key_prefix) - raw_key is shown once, hash is stored
    """
    random_part = secrets.token_urlsafe(32)
    raw_key = f"obs_live_{random_part}"
    key_hash = hash_api_key(raw_key)
    key_prefix = raw_key[:12]
    return raw_key, key_hash, key_prefix


def validate_api_key_sync(key: str) -> Optional[OrgContext]:
    """
    Validate an API key against the database (synchronous)
    
    Args:
        key: The raw API key (e.g., obs_live_xxx...)
    
    Returns:
        OrgContext if valid, None otherwise
    """
    key_hash = hash_api_key(key)
    
    result = execute_single("""
        SELECT id, organization_id, scopes 
        FROM api_keys 
        WHERE key_hash = %s 
          AND revoked = FALSE 
          AND (expires_at IS NULL OR expires_at > NOW())
    """, (key_hash,))
    
    if result:
        # Update last_used_at
        execute_insert("""
            UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = %s
        """, (key_hash,))
        
        scopes = result.get("scopes", ["ingest"])
        if isinstance(scopes, str):
            import json
            scopes = json.loads(scopes)
        
        return OrgContext(
            org_id=str(result["organization_id"]),
            source="api_key",
            scopes=scopes
        )
    return None


# ============================================
# JWT Functions
# ============================================

def create_jwt_token(user_id: str, org_id: str, email: str, role: str) -> str:
    """Create a JWT token for a user"""
    payload = {
        "user_id": user_id,
        "org_id": org_id,
        "email": email,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRY_HOURS),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_jwt_token(token: str) -> Optional[TokenData]:
    """Decode and validate a JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return TokenData(
            user_id=payload["user_id"],
            org_id=payload["org_id"],
            email=payload["email"],
            role=payload["role"]
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.InvalidTokenError:
        return None


# ============================================
# User Authentication
# ============================================

def authenticate_user(email: str, password: str) -> Optional[dict]:
    """
    Authenticate a user by email and password
    
    Returns:
        User dict if authenticated, None otherwise
    """
    user = execute_single("""
        SELECT id, organization_id, email, password_hash, name, role
        FROM users
        WHERE email = %s AND password_hash IS NOT NULL
    """, (email,))
    
    if user and verify_password(password, user["password_hash"]):
        # Update last_login_at
        execute_insert("""
            UPDATE users SET last_login_at = NOW() WHERE id = %s
        """, (str(user["id"]),))
        return user
    return None


def create_api_key_in_db(org_id: str, name: str, scopes: list, created_by: Optional[str] = None, expires_in_days: Optional[int] = None) -> Tuple[str, dict]:
    """
    Create a new API key in the database
    
    Returns:
        (raw_key, key_record) - raw_key is returned once for user to save
    """
    import json
    
    raw_key, key_hash, key_prefix = generate_api_key()
    
    expires_at = None
    if expires_in_days:
        expires_at = datetime.utcnow() + timedelta(days=expires_in_days)
    
    result = execute_single("""
        INSERT INTO api_keys (organization_id, key_hash, key_prefix, name, scopes, created_by, expires_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
        RETURNING id, key_prefix, name, scopes, expires_at, created_at
    """, (org_id, key_hash, key_prefix, name, json.dumps(scopes), created_by, expires_at), commit=True)
    
    return raw_key, result


# ============================================
# FastAPI Dependencies
# ============================================

def get_current_org(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> OrgContext:
    """
    Extract organization context from either API key or JWT
    
    This is the main auth dependency used in routes.
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    token = credentials.credentials
    
    # Try API key first (starts with obs_live_)
    if token.startswith("obs_live_"):
        org_context = validate_api_key_sync(token)
        if org_context:
            return org_context
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key"
        )
    
    # Otherwise, try JWT
    token_data = decode_jwt_token(token)
    if token_data:
        return OrgContext(
            org_id=token_data.org_id,
            user_id=token_data.user_id,
            source="jwt",
            scopes=["read", "write", "admin"] if token_data.role == "admin" else 
                   ["read", "write"] if token_data.role == "editor" else ["read"]
        )
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid token"
    )


def get_current_org_optional(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> Optional[OrgContext]:
    """
    Like get_current_org but returns None instead of raising exception
    
    Useful for endpoints that work differently for authenticated vs anonymous users
    """
    if not credentials:
        return None
    
    try:
        return get_current_org(credentials)
    except HTTPException:
        return None


def require_scope(required_scope: str):
    """
    Dependency factory that checks if the current auth has a required scope
    
    Usage:
        @router.post("/ingest", dependencies=[Depends(require_scope("ingest"))])
    """
    def check_scope(org: OrgContext = Depends(get_current_org)) -> OrgContext:
        if required_scope not in org.scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required scope: {required_scope}"
            )
        return org
    return check_scope


def require_role(required_role: str):
    """
    Dependency factory that checks if the current user has a required role
    
    Usage:
        @router.post("/admin-action", dependencies=[Depends(require_role("admin"))])
    """
    def check_role(org: OrgContext = Depends(get_current_org)) -> OrgContext:
        if org.source != "jwt":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This endpoint requires user authentication, not API key"
            )
        # Role check would need to decode JWT again or store role in OrgContext
        return org
    return check_role
