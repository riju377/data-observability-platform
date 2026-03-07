import { useState } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import {
  LayoutDashboard,
  GitBranch,
  Network,
  FileJson,
  AlertTriangle,
  Bell,
  Workflow,
  LogOut,
  User,
  Key,
  Menu,
  X,
  Sun,
  Moon,
  Monitor,
  PanelLeft,
  PanelLeftClose,
  ChevronRight
} from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { useTheme } from '../context/ThemeContext';
import './Layout.css';

function Layout() {
  const { user, logout } = useAuth();
  const { theme, toggleTheme } = useTheme();
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    return localStorage.getItem('sidebarCollapsed') === 'true';
  });
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);

  const handleLogout = () => {
    logout();
    setUserMenuOpen(false);
  };

  const toggleSidebar = () => {
    const newState = !sidebarCollapsed;
    setSidebarCollapsed(newState);
    localStorage.setItem('sidebarCollapsed', newState);
  };

  const closeMobileMenu = () => {
    setMobileMenuOpen(false);
  };

  const getUserInitial = () => {
    const email = user?.email || user?.name || 'U';
    return email.charAt(0).toUpperCase();
  };

  const getThemeIcon = () => {
    if (theme === 'light') return <Sun size={18} />;
    if (theme === 'dark') return <Moon size={18} />;
    return <Monitor size={18} />;
  };

  const getThemeLabel = () => {
    if (theme === 'light') return 'Light';
    if (theme === 'dark') return 'Dark';
    return 'System';
  };

  const navSections = [
    {
      label: 'OVERVIEW',
      items: [
        { to: '/', icon: LayoutDashboard, label: 'Dashboard', end: true },
      ]
    },
    {
      label: 'LINEAGE & SCHEMA',
      items: [
        { to: '/lineage', icon: GitBranch, label: 'Lineage' },
        { to: '/column-lineage', icon: Network, label: 'Column Lineage' },
        { to: '/schema', icon: FileJson, label: 'Schema' },
      ]
    },
    {
      label: 'MONITORING',
      items: [
        { to: '/jobs', icon: Workflow, label: 'Jobs' },
        { to: '/anomalies', icon: AlertTriangle, label: 'Anomalies' },
        { to: '/alerts', icon: Bell, label: 'Alerts' },
      ]
    },
    {
      label: 'CONFIGURATION',
      items: [
        { to: '/api-keys', icon: Key, label: 'API Keys' },
      ]
    }
  ];

  return (
    <div className="app">
      {/* Top Bar */}
      <header className="topbar">
        {/* Mobile: Hamburger menu */}
        <button
          className="topbar-menu-btn mobile-only"
          onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
          aria-label="Toggle menu"
          aria-expanded={mobileMenuOpen}
        >
          {mobileMenuOpen ? <X size={20} /> : <Menu size={20} />}
        </button>

        {/* Brand */}
        <div className="topbar-brand">
          <img src="/logo.png" alt="Logo" className="topbar-logo" />
          <h1>Data Observability</h1>
        </div>

        <div className="topbar-spacer" />

        {/* Theme Toggle with label */}
        <button
          className="topbar-theme-toggle"
          onClick={toggleTheme}
          title={`Switch to ${theme === 'light' ? 'dark' : theme === 'dark' ? 'system' : 'light'} mode`}
          aria-label="Toggle theme"
        >
          {getThemeIcon()}
          <span className="theme-label">{getThemeLabel()}</span>
        </button>

        {/* User Avatar */}
        <div className="user-menu-wrapper">
          <button
            className="user-avatar"
            onClick={() => setUserMenuOpen(!userMenuOpen)}
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
                <div className="user-dropdown-header">
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
      </header>

      {/* App Body */}
      <div className="app-body">
        {/* Sidebar */}
        <aside className={`sidebar ${sidebarCollapsed ? 'collapsed' : ''} ${mobileMenuOpen ? 'mobile-open' : ''}`}>
          {/* Sidebar toggle button */}
          <button
            className="sidebar-toggle-btn"
            onClick={toggleSidebar}
            title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            aria-label={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            aria-expanded={!sidebarCollapsed}
          >
            {sidebarCollapsed ? (
              <span className="toggle-icon-group">
                <PanelLeft size={16} />
                <ChevronRight size={12} className="toggle-arrow" />
              </span>
            ) : (
              <PanelLeftClose size={18} />
            )}
          </button>

          <nav className="sidebar-nav">
            {navSections.map((section) => (
              <div key={section.label} className="sidebar-section">
                {/* Section header */}
                <div className="sidebar-section-header">
                  <span className="sidebar-section-label">{section.label}</span>
                </div>

                {/* Section items */}
                <div className="sidebar-section-items">
                  {section.items.map(({ to, icon: Icon, label, end }) => (
                    <NavLink
                      key={to}
                      to={to}
                      end={end}
                      className="sidebar-link"
                      onClick={closeMobileMenu}
                      title={sidebarCollapsed ? label : undefined}
                    >
                      <span className="sidebar-link-icon">
                        <Icon size={20} />
                      </span>
                      <span className="sidebar-link-text">{label}</span>
                    </NavLink>
                  ))}
                </div>
              </div>
            ))}
          </nav>
        </aside>

        {/* Mobile Overlay */}
        {mobileMenuOpen && (
          <div
            className="sidebar-overlay"
            onClick={closeMobileMenu}
            aria-hidden="true"
          />
        )}

        {/* Main Content */}
        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

export default Layout;
