import { useState } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import {
  LayoutDashboard,
  GitBranch,
  Network,
  FileJson,
  AlertTriangle,
  Bell,
  Database,
  LogOut,
  User,
  Key,
  Menu,
  X,
  Sun,
  Moon,
  Monitor
} from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { useTheme } from '../context/ThemeContext';
import './Layout.css';

function Layout() {
  const { user, logout } = useAuth();
  const { theme, toggleTheme, resolvedTheme } = useTheme();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);

  const handleLogout = () => {
    logout();
    setUserMenuOpen(false);
  };

  const closeMobileMenu = () => {
    setMobileMenuOpen(false);
  };

  const toggleUserMenu = () => {
    setUserMenuOpen(!userMenuOpen);
  };

  const getUserInitial = () => {
    const email = user?.email || user?.name || 'U';
    return email.charAt(0).toUpperCase();
  };

  const getThemeIcon = () => {
    if (theme === 'light') return <Sun size={20} />;
    if (theme === 'dark') return <Moon size={20} />;
    return <Monitor size={20} />;
  };

  const getThemeLabel = () => {
    if (theme === 'light') return 'Light';
    if (theme === 'dark') return 'Dark';
    return 'System';
  };

  const navItems = [
    { to: '/', icon: LayoutDashboard, label: 'Dashboard', end: true },
    { to: '/lineage', icon: GitBranch, label: 'Lineage' },
    { to: '/column-lineage', icon: Network, label: 'Column Lineage' },
    { to: '/schema', icon: FileJson, label: 'Schema' },
    { to: '/anomalies', icon: AlertTriangle, label: 'Anomalies' },
    { to: '/alerts', icon: Bell, label: 'Alerts' },
    { to: '/api-keys', icon: Key, label: 'API Keys' },
  ];

  return (
    <div className="app">
      <nav className="navbar">
        <div className="navbar-container">
          {/* Brand */}
          <div className="nav-brand">
            <img src="/logo.png" alt="Logo" className="nav-logo" />
            <h1>Data Observability</h1>
          </div>

          {/* Desktop Navigation */}
          <div className="nav-links desktop-nav">
            {navItems.map(({ to, icon: Icon, label, end }) => (
              <NavLink
                key={to}
                to={to}
                end={end}
                className="nav-link"
                title={label}
              >
                <Icon size={20} />
                <span>{label}</span>
              </NavLink>
            ))}
          </div>

          {/* Desktop User Section */}
          <div className="nav-actions desktop-actions">
            {/* Icon-only theme toggle */}
            <button
              className="theme-toggle icon-only"
              onClick={toggleTheme}
              title={`Theme: ${getThemeLabel()} (click to change)`}
              aria-label="Toggle theme"
            >
              {getThemeIcon()}
            </button>

            {/* User avatar dropdown */}
            <div className="user-menu-wrapper">
              <button
                className="user-avatar"
                onClick={toggleUserMenu}
                title={user?.email || user?.name || 'User'}
                aria-label="User menu"
                aria-expanded={userMenuOpen}
              >
                {getUserInitial()}
              </button>
              {userMenuOpen && (
                <>
                  <div className="user-menu-backdrop" onClick={() => setUserMenuOpen(false)} />
                  <div className="user-dropdown">
                    <div className="user-dropdown-email">
                      <User size={16} />
                      <span>{user?.email || user?.name || 'User'}</span>
                    </div>
                    <button className="user-dropdown-item logout" onClick={handleLogout}>
                      <LogOut size={16} />
                      <span>Logout</span>
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>

          {/* Mobile Menu Toggle */}
          <button
            className="mobile-menu-toggle"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            aria-label="Toggle menu"
            aria-expanded={mobileMenuOpen}
          >
            {mobileMenuOpen ? <X size={24} /> : <Menu size={24} />}
          </button>
        </div>

        {/* Mobile Menu Overlay */}
        {mobileMenuOpen && (
          <div
            className="mobile-menu-overlay"
            onClick={closeMobileMenu}
            aria-hidden="true"
          />
        )}

        {/* Mobile Menu */}
        <div className={`mobile-menu ${mobileMenuOpen ? 'open' : ''}`}>
          <div className="mobile-menu-header">
            <div className="user-info-mobile" title={user?.email || user?.name || 'User'}>
              <User size={20} />
              <span className="user-email">{user?.email || user?.name || 'User'}</span>
            </div>
          </div>

          <div className="mobile-nav-links">
            {navItems.map(({ to, icon: Icon, label, end }) => (
              <NavLink
                key={to}
                to={to}
                end={end}
                className="mobile-nav-link"
                onClick={closeMobileMenu}
              >
                <Icon size={20} />
                <span>{label}</span>
              </NavLink>
            ))}
          </div>

          <div className="mobile-menu-footer">
            <button
              className="theme-toggle-mobile"
              onClick={toggleTheme}
              title={`Theme: ${getThemeLabel()}`}
            >
              {getThemeIcon()}
              <span>Theme: {getThemeLabel()}</span>
            </button>
            <button className="logout-btn-mobile" onClick={handleLogout}>
              <LogOut size={20} />
              <span>Logout</span>
            </button>
          </div>
        </div>
      </nav>

      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}

export default Layout;
