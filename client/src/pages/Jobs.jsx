import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Loader2, Zap, CheckCircle, Clock, XCircle, ArrowLeft,
  AlertTriangle, AlertOctagon, Info, ThumbsUp, Search, Workflow, Target, RefreshCw
} from 'lucide-react';
import { getJob } from '../services/api';
import { getCachedJobs, getCachedJobsSummary, getCachedJobCountries, invalidateJobsCache } from '../services/cachedApi';
import PageHeader from '../components/PageHeader';
import './Jobs.css';
import '../styles/buttons.css';
import '../styles/skeleton.css';

// ─── Utility Functions ───────────────────────────────

function formatBytes(bytes) {
  if (bytes == null || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(1024));
  const idx = Math.min(i, units.length - 1);
  return `${(bytes / Math.pow(1024, idx)).toFixed(1)} ${units[idx]}`;
}

function formatDuration(ms) {
  if (ms == null) return '—';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m}m ${rs}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm}m`;
}

function nsToMs(ns) {
  return ns != null ? ns / 1e6 : 0;
}

function formatNumber(n) {
  if (n == null) return '0';
  return Number(n).toLocaleString();
}

function formatRelativeTime(dateStr) {
  if (!dateStr) return '—';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

// ─── Metric Helpers ──────────────────────────────────

function getMetric(job, key, defaultValue = 0) {
  if (!job || !job.execution_metrics) return defaultValue;
  const val = job.execution_metrics[key];
  return val != null ? val : defaultValue;
}

function formatPercent(ratio) {
  if (ratio == null || isNaN(ratio)) return '—';
  return `${(ratio * 100).toFixed(1)}%`;
}

function calculateCpuUtilization(job) {
  const cpuNs = getMetric(job, 'executorCpuTimeNs');
  const execMs = getMetric(job, 'executionTimeMs') || job.duration_ms || 0;
  if (execMs === 0 || cpuNs === 0) return null;
  return cpuNs / (execMs * 1e6);
}

function calculateGcRatio(job) {
  const gcMs = getMetric(job, 'jvmGcTimeMs');
  const execMs = getMetric(job, 'executionTimeMs') || job.duration_ms || 0;
  if (execMs === 0) return null;
  return gcMs / execMs;
}

// ─── Optimization Signal Detection ───────────────────

function analyzeOptimizationSignals(job) {
  const signals = [];
  const m = job.execution_metrics || {};

  if (!m || Object.keys(m).length === 0) return signals;

  // 1. Disk Spill (CRITICAL)
  const diskSpill = m.diskBytesSpilled || 0;
  if (diskSpill > 0) {
    signals.push({
      type: 'Disk Spill',
      severity: 'critical',
      stage: null,
      message: `${formatBytes(diskSpill)} spilled to disk during job execution`,
      recommendation: 'Critical: Increase spark.executor.memory or reduce partition size to avoid expensive disk I/O.',
    });
  }

  // 2. Memory Spill (WARNING)
  const memSpill = m.memoryBytesSpilled || 0;
  if (memSpill > 0 && diskSpill === 0) {
    signals.push({
      type: 'Memory Spill',
      severity: 'warning',
      stage: null,
      message: `${formatBytes(memSpill)} spilled from memory (remained in-memory)`,
      recommendation: 'Consider increasing spark.executor.memory or reducing data skew.',
    });
  }

  // 3. GC Pressure (CRITICAL/WARNING)
  const execTimeMs = m.executionTimeMs || job.duration_ms || 0;
  const gcTimeMs = m.jvmGcTimeMs || 0;
  if (execTimeMs > 0) {
    const gcRatio = gcTimeMs / execTimeMs;
    if (gcRatio > 0.25) {
      signals.push({
        type: 'GC Pressure',
        severity: 'critical',
        stage: null,
        message: `GC time is ${(gcRatio * 100).toFixed(1)}% of total execution time`,
        recommendation: 'Critical: Increase executor memory significantly or tune GC settings (use G1GC).',
      });
    } else if (gcRatio > 0.10) {
      signals.push({
        type: 'GC Pressure',
        severity: 'warning',
        stage: null,
        message: `GC time is ${(gcRatio * 100).toFixed(1)}% of execution time`,
        recommendation: 'Monitor GC trends. Consider increasing executor memory if pattern persists.',
      });
    }
  }

  // 4. CPU Underutilization (WARNING/INFO)
  const cpuNs = m.executorCpuTimeNs || 0;
  if (execTimeMs > 0 && cpuNs > 0) {
    const cpuRatio = cpuNs / (execTimeMs * 1e6);
    if (cpuRatio < 0.25) {
      signals.push({
        type: 'CPU Underutilization',
        severity: 'warning',
        stage: null,
        message: `CPU utilization is only ${(cpuRatio * 100).toFixed(1)}%`,
        recommendation: 'Tasks are I/O bound. Check shuffle latency, data source performance, or increase parallelism.',
      });
    } else if (cpuRatio < 0.50) {
      signals.push({
        type: 'CPU Underutilization',
        severity: 'info',
        stage: null,
        message: `CPU utilization is ${(cpuRatio * 100).toFixed(1)}%`,
        recommendation: 'Consider increasing parallelism or investigating I/O bottlenecks.',
      });
    }
  }

  // 5. Task Locality (CRITICAL/WARNING)
  const processLocal = m.processLocalTasks || 0;
  const nodeLocal = m.nodeLocalTasks || 0;
  const rackLocal = m.rackLocalTasks || 0;
  const anyLocality = m.anyLocalityTasks || 0;
  const totalLocalityTasks = processLocal + nodeLocal + rackLocal + anyLocality;

  if (totalLocalityTasks > 0) {
    const anyPercent = (anyLocality / totalLocalityTasks) * 100;
    if (anyPercent > 30) {
      signals.push({
        type: 'Poor Task Locality',
        severity: 'critical',
        stage: null,
        message: `${anyPercent.toFixed(1)}% of tasks had ANY locality (data fetched remotely)`,
        recommendation: 'Resource contention detected. Increase executor count, add more cores, or repartition data to improve locality.',
      });
    } else if (anyPercent > 10) {
      signals.push({
        type: 'Task Locality',
        severity: 'warning',
        stage: null,
        message: `${anyPercent.toFixed(1)}% of tasks had ANY locality`,
        recommendation: 'Some tasks waited for preferred executors. Consider adding capacity or adjusting spark.locality.wait.',
      });
    }
  }

  // 6. Failed Tasks (CRITICAL)
  const failedTasks = m.failedTasks || 0;
  const totalTasks = m.totalTasks || 0;
  if (failedTasks > 0) {
    const failRate = totalTasks > 0 ? (failedTasks / totalTasks * 100) : 0;
    signals.push({
      type: 'Task Failures',
      severity: 'critical',
      stage: null,
      message: `${failedTasks} out of ${totalTasks} tasks failed (${failRate.toFixed(1)}%)`,
      recommendation: 'Review executor logs for OutOfMemory errors, data corruption, or resource contention.',
    });
  }

  return signals;
}

// ─── Time Window Options ─────────────────────────────

const TIME_WINDOWS = [
  { label: '24h', hours: 24 },
  { label: '7d', hours: 168 },
  { label: '30d', hours: 720 },
  { label: '90d', hours: 2160 },
];

// ─── Main Component ──────────────────────────────────

function Jobs() {
  // List view state
  const [jobs, setJobs] = useState([]);
  const [summary, setSummary] = useState(null);
  const [initialLoading, setInitialLoading] = useState(true);
  const [tableLoading, setTableLoading] = useState(false);
  const [statusFilter, setStatusFilter] = useState('');
  const [countryFilter, setCountryFilter] = useState('');
  const [countries, setCountries] = useState([]);
  const [nameSearch, setNameSearch] = useState('');
  const [timeWindow, setTimeWindow] = useState(24);
  const [sortField, setSortField] = useState('started_at');
  const [sortDir, setSortDir] = useState('desc');

  // Detail view state
  const [selectedJob, setSelectedJob] = useState(null);

  // Onboarding banner state
  const [bannerDismissed, setBannerDismissed] = useState(() => {
    return localStorage.getItem('onboarding-banner-dismissed') === 'true';
  });

  // Debounce ref
  const debounceRef = useRef(null);

  const dismissBanner = () => {
    setBannerDismissed(true);
    localStorage.setItem('onboarding-banner-dismissed', 'true');
  };

  // ── Load list data ──
  const loadListData = useCallback(async (status, name, hours, country, isInitial = false) => {
    if (isInitial) {
      setInitialLoading(true);
    } else {
      setTableLoading(true);
    }
    try {
      const [jobsData, summaryData] = await Promise.all([
        getCachedJobs(status || null, name || null, hours, country || null),
        getCachedJobsSummary(hours, country || null),
      ]);
      setJobs(jobsData || []);
      setSummary(summaryData || {});
    } catch (err) {
      console.error('Failed to load jobs:', err);
      setJobs([]);
      setSummary({});
    } finally {
      if (isInitial) {
        setInitialLoading(false);
      } else {
        setTableLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    getCachedJobCountries().then(setCountries).catch(() => setCountries([]));
  }, []);

  useEffect(() => {
    // Load data on mount and when filters change (not when switching to detail view)
    const isInitial = initialLoading;
    loadListData(statusFilter, nameSearch, timeWindow, countryFilter, isInitial);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter, countryFilter, timeWindow, loadListData]);

  // Debounced name search
  const handleNameSearch = (value) => {
    setNameSearch(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      loadListData(statusFilter, value, timeWindow, countryFilter, false);
    }, 300);
  };

  // Refresh handler - invalidate cache and reload jobs with current filters
  const handleRefresh = () => {
    invalidateJobsCache();
    getCachedJobCountries().then(setCountries).catch(() => setCountries([]));
    loadListData(statusFilter, nameSearch, timeWindow, countryFilter, false);
  };

  // ── Load detail data ──
  const openJobDetail = (job) => {
    setSelectedJob(job);
  };

  const closeDetail = () => {
    setSelectedJob(null);
  };

  // ── Sorting ──
  const handleSort = (field) => {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortDir('asc');
    }
  };

  const sortedJobs = [...jobs].sort((a, b) => {
    let aVal = a[sortField];
    let bVal = b[sortField];
    if (typeof aVal === 'string') aVal = aVal.toLowerCase();
    if (typeof bVal === 'string') bVal = bVal.toLowerCase();
    if (aVal < bVal) return sortDir === 'asc' ? -1 : 1;
    if (aVal > bVal) return sortDir === 'asc' ? 1 : -1;
    return 0;
  });

  // ── Render ──
  if (selectedJob) {
    return <DetailView job={selectedJob} onBack={closeDetail} />;
  }

  return <ListView
    initialLoading={initialLoading}
    tableLoading={tableLoading}
    summary={summary}
    jobs={sortedJobs}
    statusFilter={statusFilter}
    setStatusFilter={setStatusFilter}
    countryFilter={countryFilter}
    setCountryFilter={setCountryFilter}
    countries={countries}
    nameSearch={nameSearch}
    handleNameSearch={handleNameSearch}
    handleRefresh={handleRefresh}
    timeWindow={timeWindow}
    setTimeWindow={setTimeWindow}
    sortField={sortField}
    sortDir={sortDir}
    handleSort={handleSort}
    onSelectJob={openJobDetail}
    bannerDismissed={bannerDismissed}
    dismissBanner={dismissBanner}
  />;
}

// ─── List View ───────────────────────────────────────

function ListView({
  initialLoading, tableLoading, summary, jobs, statusFilter, setStatusFilter,
  countryFilter, setCountryFilter, countries,
  nameSearch, handleNameSearch, handleRefresh, timeWindow, setTimeWindow,
  sortField, sortDir, handleSort, onSelectJob,
  bannerDismissed, dismissBanner,
}) {
  const totalJobs = summary?.total_jobs ?? 0;
  const successCount = summary?.success_count ?? 0;
  const failedCount = summary?.failed_count ?? 0;
  const showOnboarding = totalJobs === 0;
  const showBanner = !bannerDismissed; // Show banner to new users even if org has jobs

  const columns = [
    { key: 'job_name', label: 'Job Name' },
    { key: 'country', label: 'Country' },
    { key: 'status', label: 'Status' },
    { key: 'started_at', label: 'Last Run' },
    { key: 'duration_ms', label: 'Duration' },
    { key: 'total_tasks', label: 'Tasks' },
    { key: 'rows_written', label: 'Rows Written' },
    { key: 'bytes_written', label: 'Data Written' },
    { key: 'failed_tasks', label: 'Failures' },
  ];

  return (
    <div className="jobs-page">
      <PageHeader
        title="Spark Jobs"
        description="Monitor job execution, performance metrics, and failures"
        icon={Workflow}
      />

      {initialLoading ? (
        <>
          {/* KPI cards skeleton */}
          <div className="skel-stat-grid" style={{ gridTemplateColumns: 'repeat(3, 1fr)' }}>
            {[0, 1, 2].map((i) => (
              <div key={i} className={`skel-stat-card skel-delay-${i}`}>
                <div className={`skel skel-icon skel-delay-${i}`} style={{ width: 44, height: 44 }} />
                <div className="skel-content">
                  <div className={`skel skel-value skel-delay-${i}`} style={{ height: 26 }} />
                  <div className={`skel skel-label skel-delay-${i}`} />
                </div>
              </div>
            ))}
          </div>
          {/* Filters skeleton */}
          <div style={{ display: 'flex', gap: '0.75rem', marginBottom: '1.5rem', alignItems: 'center' }}>
            <div className="skel skel-delay-1" style={{ width: 130, height: 38, borderRadius: 8 }} />
            <div className="skel skel-delay-1" style={{ width: 130, height: 38, borderRadius: 8 }} />
            <div className="skel skel-delay-2" style={{ width: 200, height: 38, borderRadius: 8 }} />
            <div style={{ display: 'flex', gap: '2px', marginLeft: 'auto' }}>
              {[0, 1, 2, 3].map((j) => (
                <div key={j} className={`skel skel-delay-${j}`} style={{ width: 42, height: 34, borderRadius: 6 }} />
              ))}
            </div>
          </div>
          {/* Table skeleton */}
          <div className="skel-table">
            <div className="skel-table-header" style={{ gridTemplateColumns: `repeat(${columns.length}, 1fr)` }}>
              {columns.map((col, i) => (
                <div key={i} className={`skel skel-text skel-delay-${Math.min(i, 4)}`} style={{ width: '75%' }} />
              ))}
            </div>
            {[0, 1, 2, 3, 4, 5].map((i) => (
              <div key={i} className={`skel-table-row skel-delay-${i % 4}`} style={{ gridTemplateColumns: `repeat(${columns.length}, 1fr)` }}>
                {columns.map((col, j) => (
                  <div key={j} className={`skel skel-text skel-delay-${i % 4}`} style={{ width: `${55 + (j % 3) * 15}%` }} />
                ))}
              </div>
            ))}
          </div>
        </>
      ) : (
        <>
          {/* Onboarding Banner */}
          {showBanner && (
        <div style={{
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          color: 'white',
          padding: '1.5rem',
          borderRadius: '8px',
          marginBottom: '2rem',
          position: 'relative',
        }}>
          <button
            onClick={dismissBanner}
            style={{
              position: 'absolute',
              top: '1rem',
              right: '1rem',
              background: 'rgba(255, 255, 255, 0.2)',
              border: 'none',
              color: 'white',
              borderRadius: '4px',
              padding: '0.25rem 0.5rem',
              cursor: 'pointer',
              fontSize: '0.875rem',
            }}
          >
            ✕
          </button>
          <h3 style={{ margin: '0 0 1rem 0', fontSize: '1.25rem', fontWeight: 600 }}>
            Get Started with Data Observability
          </h3>
          <div style={{ fontSize: '0.875rem', lineHeight: '1.6' }}>
            <p style={{ margin: '0 0 0.75rem 0' }}>
              <strong>Step 1:</strong> Get your API key → <a href="/api-keys" style={{ color: 'white', textDecoration: 'underline' }}>Go to API Keys</a>
            </p>
            <p style={{ margin: '0 0 0.75rem 0' }}>
              <strong>Step 2:</strong> Add to your Spark job:
            </p>
            <pre style={{
              background: 'rgba(0, 0, 0, 0.2)',
              padding: '0.75rem',
              borderRadius: '4px',
              fontSize: '0.8125rem',
              overflow: 'auto',
              margin: '0 0 0.75rem 0',
            }}>
{`spark-submit \\
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.observability.api.key="your_api_key_here" \\
  your-job.jar`}
            </pre>
            <p style={{ margin: 0 }}>
              <strong>Step 3:</strong> Run your job and metrics will appear here automatically!
            </p>
          </div>
        </div>
      )}

      {/* KPI Cards */}
      <div className="jobs-stats">
        <div className="jobs-stat-card">
          <div className="jobs-stat-icon total"><Zap size={22} /></div>
          <div>
            <div className="jobs-stat-value">{formatNumber(totalJobs)}</div>
            <div className="jobs-stat-label">Total Jobs</div>
          </div>
        </div>
        <div className="jobs-stat-card">
          <div className="jobs-stat-icon success"><CheckCircle size={22} /></div>
          <div>
            <div className="jobs-stat-value">{formatNumber(successCount)}</div>
            <div className="jobs-stat-label">Success</div>
          </div>
        </div>
        <div className="jobs-stat-card">
          <div className="jobs-stat-icon failed"><XCircle size={22} /></div>
          <div>
            <div className={`jobs-stat-value${failedCount > 0 ? ' failed-highlight' : ''}`}>
              {formatNumber(failedCount)}
            </div>
            <div className="jobs-stat-label">Failed</div>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="jobs-filters">
        <select
          className="jobs-filter-select"
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
        >
          <option value="">All Statuses</option>
          <option value="SUCCESS">Success</option>
          <option value="FAILED">Failed</option>
        </select>
        {countries.length > 0 && (
          <select
            className="jobs-filter-select"
            value={countryFilter}
            onChange={(e) => setCountryFilter(e.target.value)}
          >
            <option value="">All Countries</option>
            {countries.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        )}
        <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
          <Search size={16} style={{ position: 'absolute', left: 10, color: 'var(--text-muted)' }} />
          <input
            className="jobs-filter-input"
            type="text"
            placeholder="Search job name..."
            value={nameSearch}
            onChange={(e) => handleNameSearch(e.target.value)}
            style={{ paddingLeft: 32 }}
          />
        </div>
        <div className="jobs-time-buttons">
          {TIME_WINDOWS.map((tw) => (
            <button
              key={tw.hours}
              className={`jobs-time-btn${timeWindow === tw.hours ? ' active' : ''}`}
              onClick={() => setTimeWindow(tw.hours)}
            >
              {tw.label}
            </button>
          ))}
        </div>
        <button
          className="refresh-btn"
          onClick={handleRefresh}
          disabled={tableLoading}
          title="Refresh jobs data from database"
        >
          {tableLoading ? (
            <Loader2 size={16} className="loading-spinner" />
          ) : (
            <RefreshCw size={16} />
          )}
          <span>Refresh</span>
        </button>
      </div>

      {/* Table */}
      <div className="jobs-table-container" style={{ position: 'relative' }}>
        {tableLoading && (
          <div style={{
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0, 0, 0, 0.05)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 10,
            borderRadius: '8px',
          }}>
            <Loader2 className="jobs-loading-spinner" size={24} />
          </div>
        )}
        {jobs.length === 0 ? (
          <div className="jobs-empty">
            {showOnboarding ? (
              <>
                <Zap size={64} strokeWidth={1.5} style={{ color: '#667eea', marginBottom: '1rem' }} />
                <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '1.25rem' }}>
                  No Jobs Yet
                </h3>
                <p style={{ color: 'var(--text-muted)', margin: '0 0 1.5rem 0', maxWidth: '500px' }}>
                  Start tracking your Spark jobs by configuring the observability listener.
                  Get your API key and add it to your Spark configuration.
                </p>
                <a
                  href="/api-keys"
                  style={{
                    display: 'inline-block',
                    background: '#667eea',
                    color: 'white',
                    padding: '0.75rem 1.5rem',
                    borderRadius: '6px',
                    textDecoration: 'none',
                    fontWeight: 500,
                  }}
                >
                  Get Your API Key →
                </a>
              </>
            ) : (
              <>
                <Zap size={48} strokeWidth={1.5} />
                <p>No jobs found for the selected filters</p>
              </>
            )}
          </div>
        ) : (
          <table className="jobs-table">
            <thead>
              <tr>
                {columns.map((col) => (
                  <th
                    key={col.key}
                    className={sortField === col.key ? 'sorted' : ''}
                    onClick={() => handleSort(col.key)}
                  >
                    {col.label}
                    {sortField === col.key && (
                      <span className="sort-arrow">{sortDir === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id} onClick={() => onSelectJob(job)}>
                  <td>{job.job_name || '—'}</td>
                  <td>{job.partition_key?.match(/country=([^,]+)/)?.[1] || job.partition_key || 'GLOBAL'}</td>
                  <td>
                    <span className={`jobs-status-badge ${(job.status || '').toLowerCase()}`}>
                      {job.status || '—'}
                    </span>
                  </td>
                  <td>{formatRelativeTime(job.started_at)}</td>
                  <td>{formatDuration(job.duration_ms)}</td>
                  <td>{formatNumber(getMetric(job, 'totalTasks'))}</td>
                  <td>{formatNumber(getMetric(job, 'rowsWritten'))}</td>
                  <td>{formatBytes(getMetric(job, 'bytesWritten'))}</td>
                  <td className={getMetric(job, 'failedTasks') > 0 ? 'highlight-danger' : ''}>
                    {formatNumber(getMetric(job, 'failedTasks'))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      </>
      )}
    </div>
  );
}

// ─── Detail View ─────────────────────────────────────

function DetailView({ job, onBack }) {
  const metrics = job.execution_metrics || {};

  // Handle missing metrics
  if (!metrics || Object.keys(metrics).length === 0) {
    return (
      <div className="jobs-detail">
        <div className="jobs-detail-header">
          <button className="jobs-back-btn" onClick={onBack}>
            <ArrowLeft size={16} /> Back
          </button>
          <h2 className="jobs-detail-title">{job.job_name || 'Unnamed Job'}</h2>
        </div>
        <div className="jobs-empty" style={{ marginTop: '2rem' }}>
          <AlertTriangle size={48} strokeWidth={1.5} style={{ color: 'var(--text-muted)' }} />
          <p>No execution metrics available for this job</p>
          <small style={{ color: 'var(--text-muted)' }}>
            This job may have been executed before metrics collection was enabled
          </small>
        </div>
      </div>
    );
  }

  const signals = analyzeOptimizationSignals(job);

  // Calculate derived metrics for cards
  const cpuUtil = calculateCpuUtilization(job);
  const gcRatio = calculateGcRatio(job);

  return (
    <div className="jobs-detail">
      {/* Header */}
      <div className="jobs-detail-header">
        <button className="jobs-back-btn" onClick={onBack}>
          <ArrowLeft size={16} /> Back
        </button>
        <h2 className="jobs-detail-title">{job.job_name || 'Unnamed Job'}</h2>
        <span className={`jobs-status-badge ${(job.status || '').toLowerCase()}`}>
          {job.status || '—'}
        </span>
        <div className="jobs-detail-meta">
          <span>{formatDuration(job.duration_ms)}</span>
          {job.application_id && (
            <span className="jobs-detail-app-id">{job.application_id}</span>
          )}
        </div>
      </div>

      {/* Metric Cards Section */}
      <div className="jobs-metrics-grid">
        <MetricCard
          title="CPU Efficiency"
          value={cpuUtil ? formatPercent(cpuUtil) : '—'}
          unit=""
          icon={Zap}
          severity={cpuUtil ? (cpuUtil > 0.7 ? 'success' : cpuUtil > 0.4 ? 'warning' : 'critical') : 'info'}
        />
        <MetricCard
          title="GC Pressure"
          value={gcRatio ? formatPercent(gcRatio) : '—'}
          unit=""
          icon={AlertTriangle}
          severity={gcRatio ? (gcRatio > 0.25 ? 'critical' : gcRatio > 0.1 ? 'warning' : 'success') : 'info'}
        />
        <MetricCard
          title="Task Locality"
          value={(() => {
            const processLocal = getMetric(job, 'processLocalTasks') || 0;
            const nodeLocal = getMetric(job, 'nodeLocalTasks') || 0;
            const rackLocal = getMetric(job, 'rackLocalTasks') || 0;
            const anyLocality = getMetric(job, 'anyLocalityTasks') || 0;
            const total = processLocal + nodeLocal + rackLocal + anyLocality;

            if (total === 0) return '—';

            const good = processLocal + nodeLocal;
            return `${((good / total) * 100).toFixed(1)}% Good`;
          })()}
          unit=""
          icon={Target}
          severity={(() => {
            const processLocal = getMetric(job, 'processLocalTasks') || 0;
            const nodeLocal = getMetric(job, 'nodeLocalTasks') || 0;
            const rackLocal = getMetric(job, 'rackLocalTasks') || 0;
            const anyLocality = getMetric(job, 'anyLocalityTasks') || 0;
            const total = processLocal + nodeLocal + rackLocal + anyLocality;

            if (total === 0) return 'info';

            const anyPercent = anyLocality / total;
            return anyPercent > 0.3 ? 'critical' : anyPercent > 0.1 ? 'warning' : 'success';
          })()}
        />
        <MetricCard
          title="Max Executor Memory"
          value={(() => {
            const maxMem = getMetric(job, 'maxMemoryPerExecutorBytes');
            const configMem = job.executor_memory_mb;
            if (!maxMem) return '—';

            const memGB = maxMem / (1024 ** 3);
            if (configMem) {
              const configGB = configMem / 1024;
              const utilization = (maxMem / (configMem * 1024 * 1024));
              return `${memGB.toFixed(2)} GB (${(utilization * 100).toFixed(1)}%)`;
            }
            return formatBytes(maxMem);
          })()}
          unit=""
          icon={AlertOctagon}
          severity={(() => {
            const maxMem = getMetric(job, 'maxMemoryPerExecutorBytes');
            const configMem = job.executor_memory_mb;
            if (!maxMem || !configMem) return 'info';
            const utilization = (maxMem / (configMem * 1024 * 1024));
            return utilization > 0.9 ? 'critical' : utilization > 0.75 ? 'warning' : 'success';
          })()}
        />
      </div>

      {/* Job Metrics Summary Table */}
      <JobMetricsTable job={job} />

      {/* Optimization Signals */}
      {signals.length > 0 && (
        <div className="jobs-signals">
          <h3 className="jobs-signals-title">Optimization Signals</h3>
          <div className="jobs-signals-grid">
            {signals.map((sig, i) => (
              <div key={i} className={`jobs-signal-card ${sig.severity}`}>
                <div className="jobs-signal-header">
                  <div className="jobs-signal-icon">
                    {sig.severity === 'critical' && <AlertOctagon size={20} />}
                    {sig.severity === 'warning' && <AlertTriangle size={20} />}
                    {sig.severity === 'info' && <Info size={20} />}
                  </div>
                  <div>
                    <div className="jobs-signal-type">{sig.type}</div>
                    <span className={`jobs-signal-severity ${sig.severity}`}>{sig.severity}</span>
                  </div>
                </div>
                <div className="jobs-signal-message">{sig.message}</div>
                <div className="jobs-signal-recommendation">
                  <strong>Recommendation:</strong> {sig.recommendation}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {signals.length === 0 && (
        <div className="jobs-empty">
          <ThumbsUp size={32} strokeWidth={1.5} style={{ color: '#48bb78' }} />
          <p style={{ color: '#48bb78', fontWeight: 500 }}>No optimization issues detected</p>
        </div>
      )}
    </div>
  );
}

// ─── Helper Components ───────────────────────────────

function MetricCard({ title, value, unit, icon: Icon, severity }) {
  return (
    <div className="jobs-metric-card">
      <div className={`jobs-metric-icon ${severity}`}>
        <Icon size={20} />
      </div>
      <div>
        <div className="jobs-metric-value">{value}{unit}</div>
        <div className="jobs-metric-label">{title}</div>
      </div>
    </div>
  );
}

function JobMetricsTable({ job }) {
  const m = job.execution_metrics || {};

  return (
    <div className="jobs-metrics-sections">
      {/* Data I/O Section */}
      <div className="jobs-metrics-section">
        <h4>Data I/O</h4>
        <table className="jobs-metrics-table">
          <tbody>
            <tr><td className="label">Rows Read</td><td className="value">{formatNumber(m.rowsRead)}</td></tr>
            <tr><td className="label">Rows Written</td><td className="value">{formatNumber(m.rowsWritten)}</td></tr>
            <tr><td className="label">Bytes Read</td><td className="value">{formatBytes(m.bytesRead)}</td></tr>
            <tr><td className="label">Bytes Written</td><td className="value">{formatBytes(m.bytesWritten)}</td></tr>
          </tbody>
        </table>
      </div>

      {/* Shuffle Metrics Section */}
      <div className="jobs-metrics-section">
        <h4>Shuffle Metrics</h4>
        <table className="jobs-metrics-table">
          <tbody>
            <tr><td className="label">Shuffle Read</td><td className="value">{formatBytes(m.shuffleReadBytes)}</td></tr>
            <tr><td className="label">Shuffle Write</td><td className="value">{formatBytes(m.shuffleWriteBytes)}</td></tr>
            <tr><td className="label">  ↳ Remote Reads</td><td className="value">{formatBytes(m.shuffleRemoteBytesRead)}</td></tr>
            <tr><td className="label">  ↳ Local Reads</td><td className="value">{formatBytes(m.shuffleLocalBytesRead)}</td></tr>
          </tbody>
        </table>
      </div>

      {/* Resource Utilization Section */}
      <div className="jobs-metrics-section">
        <h4>Resource Utilization</h4>
        <table className="jobs-metrics-table">
          <tbody>
            <tr><td className="label">Total Tasks</td><td className="value">{formatNumber(m.totalTasks)}</td></tr>
            <tr className={(m.failedTasks || 0) > 0 ? 'highlight' : ''}>
              <td className="label">Failed Tasks</td>
              <td className="value">{formatNumber(m.failedTasks)}</td>
            </tr>

            {/* NEW: Executor-level memory metrics */}
            {m.maxMemoryPerExecutorBytes && (
              <tr>
                <td className="label">
                  Max Memory Per Executor
                  <span className="label-hint"> (actionable for tuning)</span>
                </td>
                <td className="value">
                  {(() => {
                    const maxMem = m.maxMemoryPerExecutorBytes;
                    const configMem = job.executor_memory_mb;
                    const memGB = maxMem / (1024 ** 3);

                    if (configMem) {
                      const configGB = configMem / 1024;
                      const utilization = (maxMem / (configMem * 1024 * 1024));
                      return (
                        <span className={utilization > 0.9 ? 'text-critical' : utilization > 0.75 ? 'text-warning' : ''}>
                          {memGB.toFixed(2)} GB / {configGB.toFixed(0)} GB ({(utilization * 100).toFixed(1)}%)
                        </span>
                      );
                    }
                    return formatBytes(maxMem);
                  })()}
                </td>
              </tr>
            )}

            {m.maxMemoryPerCoreBytes && (
              <tr>
                <td className="label">Max Memory Per Core</td>
                <td className="value">{formatBytes(m.maxMemoryPerCoreBytes)}</td>
              </tr>
            )}

            {m.avgMemoryPerExecutorBytes && (
              <tr>
                <td className="label">Avg Executor Memory</td>
                <td className="value">{formatBytes(m.avgMemoryPerExecutorBytes)}</td>
              </tr>
            )}

            {m.executorCount && (
              <tr>
                <td className="label">Executor Count</td>
                <td className="value">{m.executorCount}</td>
              </tr>
            )}

            {/* Task Locality Metrics */}
            {(m.processLocalTasks !== undefined || m.nodeLocalTasks !== undefined ||
              m.rackLocalTasks !== undefined || m.anyLocalityTasks !== undefined) && (
              <>
                <tr>
                  <td colSpan="2" style={{ paddingTop: '1rem', fontWeight: 'bold', color: 'var(--text-primary)' }}>
                    Task Locality Distribution
                  </td>
                </tr>
                {m.processLocalTasks !== undefined && (
                  <tr>
                    <td className="label">
                      PROCESS_LOCAL
                      <span className="label-hint"> (best - data in same JVM)</span>
                    </td>
                    <td className="value text-success">{formatNumber(m.processLocalTasks)}</td>
                  </tr>
                )}
                {m.nodeLocalTasks !== undefined && (
                  <tr>
                    <td className="label">
                      NODE_LOCAL
                      <span className="label-hint"> (data on same node)</span>
                    </td>
                    <td className="value">{formatNumber(m.nodeLocalTasks)}</td>
                  </tr>
                )}
                {m.rackLocalTasks !== undefined && (
                  <tr>
                    <td className="label">
                      RACK_LOCAL
                      <span className="label-hint"> (data in same rack)</span>
                    </td>
                    <td className="value">{formatNumber(m.rackLocalTasks)}</td>
                  </tr>
                )}
                {m.anyLocalityTasks !== undefined && (
                  <tr className={m.anyLocalityTasks > 0 ? 'highlight' : ''}>
                    <td className="label">
                      ANY
                      <span className="label-hint"> (worst - data fetched remotely)</span>
                    </td>
                    <td className="value">
                      <span className={m.anyLocalityTasks > 0 ? 'text-critical' : ''}>
                        {formatNumber(m.anyLocalityTasks)}
                        {(() => {
                          const total = (m.processLocalTasks || 0) + (m.nodeLocalTasks || 0) +
                                        (m.rackLocalTasks || 0) + (m.anyLocalityTasks || 0);
                          if (total > 0 && m.anyLocalityTasks > 0) {
                            const anyPercent = (m.anyLocalityTasks / total) * 100;
                            return ` (${anyPercent.toFixed(1)}%)`;
                          }
                          return '';
                        })()}
                      </span>
                    </td>
                  </tr>
                )}
              </>
            )}

            {/* OLD: Keep deprecated peak memory for comparison (grayed out) */}
            {m.peakExecutionMemory && !m.maxMemoryPerExecutorBytes && (
              <tr style={{ opacity: 0.5 }}>
                <td className="label">
                  Peak Memory
                  <span className="label-hint"> (deprecated - sum of all tasks)</span>
                </td>
                <td className="value">{formatBytes(m.peakExecutionMemory)}</td>
              </tr>
            )}

            <tr><td className="label">CPU Time</td><td className="value">{formatDuration(nsToMs(m.executorCpuTimeNs))}</td></tr>
            <tr><td className="label">GC Time</td><td className="value">{formatDuration(m.jvmGcTimeMs)}</td></tr>
          </tbody>
        </table>
      </div>

      {/* Spill Metrics Section */}
      <div className="jobs-metrics-section">
        <h4>Spill Metrics</h4>
        <table className="jobs-metrics-table">
          <tbody>
            <tr className={(m.memoryBytesSpilled || 0) > 0 ? 'highlight' : ''}>
              <td className="label">Memory Spilled</td>
              <td className="value">{formatBytes(m.memoryBytesSpilled)}</td>
            </tr>
            <tr className={(m.diskBytesSpilled || 0) > 0 ? 'highlight' : ''}>
              <td className="label">Disk Spilled</td>
              <td className="value">{formatBytes(m.diskBytesSpilled)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default Jobs;
