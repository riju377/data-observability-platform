import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext';
import { ThemeProvider } from './context/ThemeContext';
import { ToastProvider } from './context/ToastContext';
import { ToastContainer } from './components/Toast';
import ProtectedRoute from './components/ProtectedRoute';
import Layout from './components/Layout';
import Login from './pages/Login';
import Register from './pages/Register';
import Dashboard from './pages/Dashboard';
import Lineage from './pages/Lineage';
import ColumnLineage from './pages/ColumnLineage';
import Schema from './pages/Schema';
import Anomalies from './pages/Anomalies';
import Alerts from './pages/Alerts';
import ApiKeys from './pages/ApiKeys';
import './App.css';

function App() {
  return (
    <ThemeProvider>
      <ToastProvider>
        <AuthProvider>
          <BrowserRouter>
            <ToastContainer />
            <Routes>
              {/* Public routes */}
              <Route path="/login" element={<Login />} />
              <Route path="/register" element={<Register />} />

              {/* Protected routes */}
              <Route
                path="/"
                element={
                  <ProtectedRoute>
                    <Layout />
                  </ProtectedRoute>
                }
              >
                <Route index element={<Dashboard />} />
                <Route path="lineage" element={<Lineage />} />
                <Route path="column-lineage" element={<ColumnLineage />} />
                <Route path="schema" element={<Schema />} />
                <Route path="anomalies" element={<Anomalies />} />
                <Route path="alerts" element={<Alerts />} />
                <Route path="api-keys" element={<ApiKeys />} />
              </Route>
            </Routes>
          </BrowserRouter>
        </AuthProvider>
      </ToastProvider>
    </ThemeProvider>
  );
}

export default App;