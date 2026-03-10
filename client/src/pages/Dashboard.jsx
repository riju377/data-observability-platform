import { useState, useEffect } from 'react';
import { getCachedDatasets, getCachedAnomalies, getCachedAlertHistory } from '../services/cachedApi';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';
import { Database, AlertTriangle, Bell, TrendingUp, LayoutDashboard } from 'lucide-react';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import GettingStarted from '../components/GettingStarted';
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
      console.log('🔍 Dashboard: Starting to load data...');
      // Use Promise.allSettled so one failing endpoint doesn't break everything
      const results = await Promise.allSettled([
        getCachedDatasets(),
        getCachedAnomalies(168, null, 100),
        getCachedAlertHistory(168),
      ]);

      // Extract successful results, use empty arrays for failures
      const datasets = results[0].status === 'fulfilled' ? results[0].value : [];
      const anomalies = results[1].status === 'fulfilled' ? results[1].value : [];
      const alerts = results[2].status === 'fulfilled' ? results[2].value : [];

      // Log any failures
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          const names = ['datasets', 'anomalies', 'alerts'];
          console.warn(`⚠️ Dashboard: Failed to load ${names[index]}:`, result.reason);
        }
      });

      console.log('🔍 Dashboard: Data received:', {
        datasets: datasets,
        datasetsType: typeof datasets,
        datasetsIsArray: Array.isArray(datasets),
        datasetsLength: datasets?.length,
        firstDataset: datasets?.[0],
        anomalies: anomalies?.length,
        alerts: alerts?.length,
      });

      const newStats = {
        datasets: datasets?.length || 0,
        anomalies: anomalies?.length || 0,
        alerts: alerts?.length || 0,
      };

      console.log('🔍 Dashboard: Setting stats:', newStats);
      setStats(newStats);

      const severityCount = (anomalies || []).reduce((acc, a) => {
        acc[a.severity] = (acc[a.severity] || 0) + 1;
        return acc;
      }, {});

      setAnomaliesBySeverity(
        Object.entries(severityCount).map(([name, value]) => ({ name, value }))
      );
      setLoading(false);
    } catch (error) {
      console.error('🔴 Dashboard: ERROR loading data:', error);
      console.error('🔴 Dashboard: Error details:', {
        message: error.message,
        stack: error.stack,
        response: error.response
      });
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

  return (
    <div className="dashboard">
      <PageHeader
        title="Dashboard Overview"
        description="Monitor your data health at a glance"
        icon={LayoutDashboard}
      />

      {(() => {
        console.log('🔍 Dashboard: Render check:', { loading, stats, showingGuide: stats.datasets === 0 });
        if (loading) {
          return <LoadingSpinner message="Loading dashboard..." />;
        }
        if (stats.datasets === 0) {
          console.log('⚠️ Dashboard: Showing integration guide because stats.datasets === 0');
          return <GettingStarted />;
        }
        console.log('✅ Dashboard: Showing dashboard with', stats.datasets, 'datasets');
        return (
        <>
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
        </>
        );
      })()}
    </div>
  );
}

export default Dashboard;