import { Navigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

/**
 * Wrapper component that redirects to login if not authenticated
 * Uses optimistic rendering - shows navbar immediately with cached user data
 *
 * Usage:
 *   <ProtectedRoute>
 *     <Dashboard />
 *   </ProtectedRoute>
 */
function ProtectedRoute({ children }) {
    const { isAuthenticated, validating } = useAuth();
    const location = useLocation();

    // Only redirect if definitely not authenticated
    // If validating, render children (optimistic rendering)
    if (!isAuthenticated && !validating) {
        // Redirect to login, saving the attempted URL
        return <Navigate to="/login" state={{ from: location }} replace />;
    }

    // Render children immediately - navbar shows with cached user data
    return children;
}

export default ProtectedRoute;
