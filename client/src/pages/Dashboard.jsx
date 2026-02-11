import { useState, useEffect } from 'react';
import { getDatasets, getAnomalies, getAlertHistory } from '../services/api';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';
import { Database, AlertTriangle, Bell, TrendingUp, LayoutDashboard } from 'lucide-react';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './Dashboard.css';

function Dashboard() {
  const [stats, setStats] = useState({ datasets: 0, anomalies: 0, alerts: 0 });
  const [anomaliesBySeverity, setAnomaliesBySeverity] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      const [datasetsRes, anomaliesRes, alertsRes] = await Promise.all([
        getDatasets(),
        getAnomalies(168, null, 100),
        getAlertHistory(168),
      ]);

      setStats({
        datasets: datasetsRes.data?.length || 0,
        anomalies: anomaliesRes.data?.length || 0,
        alerts: alertsRes.data?.length || 0,
      });

      const severityCount = (anomaliesRes.data || []).reduce((acc, a) => {
        acc[a.severity] = (acc[a.severity] || 0) + 1;
        return acc;
      }, {});

      setAnomaliesBySeverity(
        Object.entries(severityCount).map(([name, value]) => ({ name, value }))
      );
      setLoading(false);
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
      setStats({ datasets: 0, anomalies: 0, alerts: 0 });
      setAnomaliesBySeverity([]);
      setLoading(false);
    }
  };

  const COLORS = {
    CRITICAL: '#d32f2f',
    WARNING: '#f57c00',
    INFO: '#1976d2',
  };

  if (loading) {
    return <LoadingSpinner message="Loading dashboard..." />;
  }

  return (
    <div className="dashboard">
      <PageHeader
        title="Dashboard Overview"
        description="Monitor your data health at a glance"
        icon={LayoutDashboard}
      />

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon datasets">
            <Database size={24} />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats.datasets}</div>
            <div className="stat-label">Total Datasets</div>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon anomalies">
            <AlertTriangle size={24} />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats.anomalies}</div>
            <div className="stat-label">Anomalies (7d)</div>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon alerts">
            <Bell size={24} />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats.alerts}</div>
            <div className="stat-label">Alerts Sent (7d)</div>
          </div>
        </div>
      </div>

      <div className="charts">
        <div className="chart-card">
          <div className="chart-header">
            <TrendingUp size={20} />
            <h3>Anomalies by Severity</h3>
          </div>
          {anomaliesBySeverity.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={anomaliesBySeverity}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  outerRadius={100}
                  innerRadius={60}
                  paddingAngle={2}
                  label={({ name, value }) => `${name}: ${value}`}
                  labelLine={false}
                >
                  {anomaliesBySeverity.map((entry) => (
                    <Cell key={entry.name} fill={COLORS[entry.name]} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    background: 'var(--surface)',
                    border: '1px solid var(--border)',
                    borderRadius: '8px',
                    boxShadow: 'var(--shadow-md)'
                  }}
                />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="chart-empty">
              <AlertTriangle size={48} strokeWidth={1.5} />
              <p>No anomalies detected in the last 7 days</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default Dashboard;