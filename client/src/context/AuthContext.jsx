import { createContext, useContext, useState, useEffect } from 'react';
import { apiCache } from '../utils/cache';

const AuthContext = createContext(null);

const API_BASE = import.meta.env.VITE_API_URL;

// User cache helpers
const CACHE_KEY = 'user_cache';
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

const getCachedUser = () => {
  const cached = localStorage.getItem(CACHE_KEY);
  if (!cached) return null;

  try {
    const { user, timestamp } = JSON.parse(cached);
    if (Date.now() - timestamp > CACHE_TTL) {
      localStorage.removeItem(CACHE_KEY);
      return null;
    }
    return user;
  } catch {
    return null;
  }
};

const setCachedUser = (user) => {
  localStorage.setItem(CACHE_KEY, JSON.stringify({
    user,
    timestamp: Date.now()
  }));
};

const clearCachedUser = () => {
  localStorage.removeItem(CACHE_KEY);
};

export function AuthProvider({ children}) {
    const [user, setUser] = useState(getCachedUser()); // Use cached user!
    const [token, setToken] = useState(localStorage.getItem('token'));
    const [loading, setLoading] = useState(false); // Changed from true
    const [validating, setValidating] = useState(!!localStorage.getItem('token')); // New state

    // Check if token is valid on mount
    useEffect(() => {
        if (token) {
            validateToken();
        } else {
            setLoading(false);
        }
    }, []);

    const validateToken = async () => {
        setValidating(true);
        try {
            const response = await fetch(`${API_BASE}/api/v1/auth/me`, {
                headers: { Authorization: `Bearer ${token}` }
            });
            if (response.ok) {
                const data = await response.json();
                const userData = data.user || data;
                setUser(userData);
                setCachedUser(userData); // Cache it
            } else {
                // Token invalid, clear it
                logout();
            }
        } catch (error) {
            console.error('Token validation failed:', error);
            logout();
        } finally {
            setValidating(false);
        }
    };

    const login = async (email, password) => {
        const response = await fetch(`${API_BASE}/api/v1/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, password })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Login failed');
        }

        const data = await response.json();
        setToken(data.access_token);
        setUser(data.user);
        localStorage.setItem('token', data.access_token);
        setCachedUser(data.user); // Cache user
        return data;
    };

    const logout = () => {
        setToken(null);
        setUser(null);
        localStorage.removeItem('token');
        clearCachedUser(); // Clear user cache
        apiCache.clear(); // Clear API cache
    };

    const getAuthHeaders = () => {
        if (!token) return {};
        return { Authorization: `Bearer ${token}` };
    };

    const value = {
        user,
        token,
        loading,
        validating,
        isAuthenticated: !!token,
        login,
        logout,
        getAuthHeaders
    };

    return (
        <AuthContext.Provider value={value}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
}

export default AuthContext;
