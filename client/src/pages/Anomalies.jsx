import { useState, useEffect } from 'react';
import { getCachedAnomalies, invalidateMonitoringCache } from '../services/cachedApi';
import { AlertTriangle, AlertCircle, Info, Clock, Database, Filter, Loader2, RefreshCw } from 'lucide-react';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import SchemaDiffViewer from '../components/SchemaDiffViewer';
import AnomalyMetricComparison from '../components/AnomalyMetricComparison';
import './Anomalies.css';
import '../styles/buttons.css';

function Anomalies() {
  const [anomalies, setAnomalies] = useState([]);
  const [filter, setFilter] = useState('ALL');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadAnomalies();
  }, [filter]);

  const loadAnomalies = async () => {
    setLoading(true);
    try {
      const severity = filter === 'ALL' ? null : filter;
      const anomaliesData = await getCachedAnomalies(168, severity, 100);
      setAnomalies(anomaliesData || []);
    } catch (error) {
      console.error('Failed to load anomalies:', error);
      setAnomalies([]);
    }
    setLoading(false);
  };

  // Refresh handler - invalidate cache and reload
  const handleRefresh = () => {
    invalidateMonitoringCache();
    loadAnomalies();
  };

  const getSeverityClass = (severity) => severity.toLowerCase();

  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'CRITICAL':
        return <AlertCircle size={16} />;
      case 'WARNING':
        return <AlertTriangle size={16} />;
      case 'INFO':
      default:
        return <Info size={16} />;
    }
  };

  const formatDate = (dateString) => {
    const date = new Date(dateString);
    const now = new Date();
    const diff = now - date;
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const days = Math.floor(hours / 24);

    if (days > 0) return `${days}d ago`;
    if (hours > 0) return `${hours}h ago`;
    return 'Just now';
  };

  return (
    <div className="anomalies">
      <PageHeader
        title="Anomalies"
        description="Detected anomalies in the last 7 days"
        icon={AlertTriangle}
      >
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
          <button className="refresh-btn" onClick={handleRefresh} disabled={loading} title="Refresh anomalies from database">
            {loading ? (
              <Loader2 size={16} className="loading-spinner" />
            ) : (
              <RefreshCw size={16} />
            )}
            <span>Refresh</span>
          </button>
          <div className="filter-section">
            <Filter size={18} />
            <div className="filter-buttons">
              {['ALL', 'CRITICAL', 'WARNING', 'INFO'].map((s) => (
                <button
                  key={s}
                  className={`filter-btn ${filter === s ? 'active' : ''} ${s.toLowerCase()}`}
                  onClick={() => setFilter(s)}
                >
                  {s !== 'ALL' && getSeverityIcon(s)}
                  <span>{s}</span>
                </button>
              ))}
            </div>
          </div>
        </div>
      </PageHeader>

      {loading ? (
        <div className="loading">
          <Loader2 className="loading-spinner" size={32} />
          <span>Loading anomalies...</span>
        </div>
      ) : anomalies.length === 0 ? (
        <div className="empty-state">
          <AlertTriangle size={48} strokeWidth={1.5} />
          <h3>No anomalies found</h3>
          <p>Great news! No anomalies detected with the current filter.</p>
        </div>
      ) : (
        <div className="anomalies-list">
          {anomalies.map((a) => (
            <div key={a.id} className={`anomaly-card ${getSeverityClass(a.severity)}`}>
              <div className="anomaly-header">
                <span className={`severity-badge ${getSeverityClass(a.severity)}`}>
                  {getSeverityIcon(a.severity)}
                  <span>{a.severity}</span>
                </span>
                <span className="dataset-tag">
                  <Database size={14} />
                  <span>{a.dataset_name}</span>
                </span>
              </div>
              <div className="anomaly-body">
                <h4 className="anomaly-type">{a.anomaly_type}</h4>
                {a.anomaly_type === 'SchemaChange' ? (
                  <SchemaDiffViewer
                    diff={a.actual_value?.diff}
                    description={a.description}
                  />
                ) : (a.anomaly_type.includes('RowCount') || a.anomaly_type.includes('Volume')) ? (
                  <>
                    <p className="anomaly-description">{a.description}</p>
                    <AnomalyMetricComparison
                      actual={a.actual_value}
                      expected={a.expected_value}
                      type={a.anomaly_type}
                      severity={a.severity}
                    />
                  </>
                ) : (
                  <p className="anomaly-description">{a.description}</p>
                )}
              </div>
              <div className="anomaly-footer">
                <Clock size={14} />
                <span>{formatDate(a.detected_at)}</span>
                <span className="separator">|</span>
                <span className="full-date">{new Date(a.detected_at).toLocaleString()}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default Anomalies;