import { useState, useEffect, useCallback, useRef } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell
} from 'recharts';
import {
  Loader2, Zap, CheckCircle, Clock, XCircle, ArrowLeft,
  AlertTriangle, AlertOctagon, Info, ThumbsUp, Search
} from 'lucide-react';
import { getJobs, getJob, getJobStages, getJobsSummary } from '../services/api';
import './Jobs.css';

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

const TOOLTIP_STYLE = {
  background: 'var(--surface)',
  border: '1px solid var(--border)',
  borderRadius: '8px',
  boxShadow: 'var(--shadow-md)',
};

// ─── Optimization Signal Detection ───────────────────

function analyzeOptimizationSignals(stages) {
  const signals = [];
  if (!stages || stages.length === 0) return signals;

  for (const stage of stages) {
    const stageLabel = `Stage ${stage.stage_id ?? stage.stage_attempt_id ?? '?'}`;

    // Disk spill
    if ((stage.disk_bytes_spilled || 0) > 0) {
      signals.push({
        type: 'Disk Spill',
        severity: 'critical',
        stage: stageLabel,
        message: `${stageLabel}: ${formatBytes(stage.disk_bytes_spilled)} spilled to disk`,
        recommendation: 'Increase executor memory or reduce partition size to avoid disk spill.',
      });
    }

    // Memory spill (only if no disk spill)
    if ((stage.memory_bytes_spilled || 0) > 0 && (stage.disk_bytes_spilled || 0) === 0) {
      signals.push({
        type: 'Memory Spill',
        severity: 'warning',
        stage: stageLabel,
        message: `${stageLabel}: ${formatBytes(stage.memory_bytes_spilled)} memory spill detected`,
        recommendation: 'Consider increasing spark.executor.memory or reducing data skew.',
      });
    }

    // GC pressure
    const runTime = stage.executor_run_time_ms || 0;
    const gcTime = stage.jvm_gc_time_ms || 0;
    if (runTime > 0) {
      const gcRatio = gcTime / runTime;
      if (gcRatio > 0.25) {
        signals.push({
          type: 'GC Pressure',
          severity: 'critical',
          stage: stageLabel,
          message: `${stageLabel}: GC time is ${(gcRatio * 100).toFixed(1)}% of run time`,
          recommendation: 'Increase executor memory or tune GC settings (G1GC, heap ratios).',
        });
      } else if (gcRatio > 0.10) {
        signals.push({
          type: 'GC Pressure',
          severity: 'warning',
          stage: stageLabel,
          message: `${stageLabel}: GC time is ${(gcRatio * 100).toFixed(1)}% of run time`,
          recommendation: 'Monitor GC trends. Consider increasing executor memory if worsening.',
        });
      }
    }

    // CPU underutilization
    const cpuNs = stage.executor_cpu_time_ns || 0;
    const runMs = stage.executor_run_time_ms || 0;
    if (runMs > 0 && cpuNs > 0) {
      const cpuRatio = cpuNs / (runMs * 1e6);
      if (cpuRatio < 0.25) {
        signals.push({
          type: 'CPU Underutil',
          severity: 'warning',
          stage: stageLabel,
          message: `${stageLabel}: CPU utilization is only ${(cpuRatio * 100).toFixed(1)}%`,
          recommendation: 'Tasks may be I/O bound. Check shuffle read or data source latency.',
        });
      } else if (cpuRatio < 0.50) {
        signals.push({
          type: 'CPU Underutil',
          severity: 'info',
          stage: stageLabel,
          message: `${stageLabel}: CPU utilization is ${(cpuRatio * 100).toFixed(1)}%`,
          recommendation: 'Consider increasing parallelism or checking for I/O bottlenecks.',
        });
      }
    }

    // Shuffle skew (remote vs total read)
    const remoteRead = (stage.shuffle_remote_bytes_read || 0);
    const localRead = (stage.shuffle_local_bytes_read || 0);
    const totalRead = remoteRead + localRead;
    if (totalRead > 0) {
      const remoteRatio = remoteRead / totalRead;
      if (remoteRatio > 0.95) {
        signals.push({
          type: 'Shuffle Skew',
          severity: 'warning',
          stage: stageLabel,
          message: `${stageLabel}: ${(remoteRatio * 100).toFixed(1)}% of shuffle reads are remote`,
          recommendation: 'Enable locality-aware scheduling or increase data replication.',
        });
      } else if (remoteRatio > 0.80) {
        signals.push({
          type: 'Shuffle Skew',
          severity: 'info',
          stage: stageLabel,
          message: `${stageLabel}: ${(remoteRatio * 100).toFixed(1)}% of shuffle reads are remote`,
          recommendation: 'Consider co-locating data or adjusting shuffle partitions.',
        });
      }
    }
  }

  return signals;
}

const SIGNAL_ICONS = {
  critical: AlertOctagon,
  warning: AlertTriangle,
  info: Info,
};

// ─── Time Window Options ─────────────────────────────

const TIME_WINDOWS = [
  { label: '6h', hours: 6 },
  { label: '12h', hours: 12 },
  { label: '24h', hours: 24 },
  { label: '48h', hours: 48 },
  { label: '7d', hours: 168 },
];

// ─── Chart Colors ────────────────────────────────────

const BREAKDOWN_COLORS = {
  compute: '#667eea',
  gc: '#fc8181',
  deserialization: '#ed8936',
  shuffleWait: '#63b3ed',
  serialization: '#a0aec0',
};

const DURATION_BAR_COLORS = [
  '#667eea', '#48bb78', '#ed8936', '#fc8181', '#9f7aea',
  '#38b2ac', '#f6ad55', '#63b3ed', '#e53e3e', '#68d391',
];

// ─── Main Component ──────────────────────────────────

function Jobs() {
  // List view state
  const [jobs, setJobs] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState('');
  const [nameSearch, setNameSearch] = useState('');
  const [timeWindow, setTimeWindow] = useState(24);
  const [sortField, setSortField] = useState('started_at');
  const [sortDir, setSortDir] = useState('desc');

  // Detail view state
  const [selectedJob, setSelectedJob] = useState(null);
  const [stages, setStages] = useState([]);
  const [stagesLoading, setStagesLoading] = useState(false);

  // Debounce ref
  const debounceRef = useRef(null);

  // ── Load list data ──
  const loadListData = useCallback(async (status, name, hours) => {
    setLoading(true);
    try {
      const [jobsRes, summaryRes] = await Promise.all([
        getJobs(status || null, name || null, hours),
        getJobsSummary(hours),
      ]);
      setJobs(jobsRes.data || []);
      setSummary(summaryRes.data || {});
    } catch (err) {
      console.error('Failed to load jobs:', err);
      setJobs([]);
      setSummary({});
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!selectedJob) {
      loadListData(statusFilter, nameSearch, timeWindow);
    }
  }, [statusFilter, timeWindow, selectedJob, loadListData]);

  // Debounced name search
  const handleNameSearch = (value) => {
    setNameSearch(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      loadListData(statusFilter, value, timeWindow);
    }, 300);
  };

  // ── Load detail data ──
  const openJobDetail = async (job) => {
    setSelectedJob(job);
    setStagesLoading(true);
    try {
      const res = await getJobStages(job.id);
      setStages(res.data || []);
    } catch (err) {
      console.error('Failed to load stages:', err);
      setStages([]);
    } finally {
      setStagesLoading(false);
    }
  };

  const closeDetail = () => {
    setSelectedJob(null);
    setStages([]);
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
    return <DetailView job={selectedJob} stages={stages} stagesLoading={stagesLoading} onBack={closeDetail} />;
  }

  return <ListView
    loading={loading}
    summary={summary}
    jobs={sortedJobs}
    statusFilter={statusFilter}
    setStatusFilter={setStatusFilter}
    nameSearch={nameSearch}
    handleNameSearch={handleNameSearch}
    timeWindow={timeWindow}
    setTimeWindow={setTimeWindow}
    sortField={sortField}
    sortDir={sortDir}
    handleSort={handleSort}
    onSelectJob={openJobDetail}
  />;
}

// ─── List View ───────────────────────────────────────

function ListView({
  loading, summary, jobs, statusFilter, setStatusFilter,
  nameSearch, handleNameSearch, timeWindow, setTimeWindow,
  sortField, sortDir, handleSort, onSelectJob,
}) {
  if (loading) {
    return (
      <div className="jobs-loading">
        <Loader2 className="jobs-loading-spinner" size={32} />
        <span>Loading jobs...</span>
      </div>
    );
  }

  const totalJobs = summary?.total_jobs ?? 0;
  const successRate = summary?.success_rate != null ? `${Number(summary.success_rate).toFixed(1)}%` : '—';
  const avgDuration = formatDuration(summary?.avg_duration_ms);
  const failedCount = summary?.failed_count ?? 0;

  const columns = [
    { key: 'job_name', label: 'Job Name' },
    { key: 'status', label: 'Status' },
    { key: 'started_at', label: 'Started' },
    { key: 'duration_ms', label: 'Duration' },
    { key: 'total_tasks', label: 'Tasks' },
    { key: 'shuffle_read_bytes', label: 'Shuffle Read' },
    { key: 'shuffle_write_bytes', label: 'Shuffle Write' },
  ];

  return (
    <div className="jobs-page">
      <div className="jobs-header">
        <h2>Jobs</h2>
      </div>

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
            <div className="jobs-stat-value">{successRate}</div>
            <div className="jobs-stat-label">Success Rate</div>
          </div>
        </div>
        <div className="jobs-stat-card">
          <div className="jobs-stat-icon duration"><Clock size={22} /></div>
          <div>
            <div className="jobs-stat-value">{avgDuration}</div>
            <div className="jobs-stat-label">Avg Duration</div>
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
          <option value="RUNNING">Running</option>
        </select>
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
      </div>

      {/* Table */}
      <div className="jobs-table-container">
        {jobs.length === 0 ? (
          <div className="jobs-empty">
            <Zap size={48} strokeWidth={1.5} />
            <p>No jobs found for the selected filters</p>
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
                  <td>
                    <span className={`jobs-status-badge ${(job.status || '').toLowerCase()}`}>
                      {job.status || '—'}
                    </span>
                  </td>
                  <td>{formatRelativeTime(job.started_at)}</td>
                  <td>{formatDuration(job.duration_ms)}</td>
                  <td>{formatNumber(job.total_tasks)}</td>
                  <td>{formatBytes(job.shuffle_read_bytes)}</td>
                  <td>{formatBytes(job.shuffle_write_bytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ─── Detail View ─────────────────────────────────────

function DetailView({ job, stages, stagesLoading, onBack }) {
  if (stagesLoading) {
    return (
      <div className="jobs-loading">
        <Loader2 className="jobs-loading-spinner" size={32} />
        <span>Loading stage metrics...</span>
      </div>
    );
  }

  const signals = analyzeOptimizationSignals(stages);

  // Chart A: Duration per stage
  const durationData = stages.map((s) => ({
    name: `Stage ${s.stage_id ?? '?'}`,
    duration_ms: s.duration_ms || s.executor_run_time_ms || 0,
  }));

  // Chart B: Time breakdown per stage
  const breakdownData = stages.map((s) => {
    const runTime = s.executor_run_time_ms || 0;
    const gcTime = s.jvm_gc_time_ms || 0;
    const deserTime = s.executor_deserialize_time_ms || 0;
    const shuffleWait = (s.shuffle_fetch_wait_time_ms || 0) + nsToMs(s.shuffle_write_time_ns || 0);
    const serTime = s.result_serialization_time_ms || 0;
    const compute = Math.max(0, runTime - gcTime);
    return {
      name: `Stage ${s.stage_id ?? '?'}`,
      Compute: compute,
      GC: gcTime,
      Deserialization: deserTime,
      'Shuffle Wait': shuffleWait,
      Serialization: serTime,
    };
  });

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

      {/* Charts */}
      <div className="jobs-charts-grid">
        {/* Chart A: Duration per Stage */}
        <div className="jobs-chart-card">
          <h3 className="jobs-chart-title">Duration per Stage</h3>
          {durationData.length > 0 ? (
            <ResponsiveContainer width="100%" height={Math.max(200, durationData.length * 40)}>
              <BarChart layout="vertical" data={durationData} margin={{ top: 5, right: 30, left: 60, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis
                  type="number"
                  tickFormatter={(v) => formatDuration(v)}
                  stroke="var(--text-muted)"
                  fontSize={12}
                />
                <YAxis
                  type="category"
                  dataKey="name"
                  stroke="var(--text-muted)"
                  fontSize={12}
                  width={80}
                />
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(v) => formatDuration(v)}
                />
                <Bar dataKey="duration_ms" name="Duration" radius={[0, 4, 4, 0]}>
                  {durationData.map((_, i) => (
                    <Cell key={i} fill={DURATION_BAR_COLORS[i % DURATION_BAR_COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="jobs-empty"><p>No stage data available</p></div>
          )}
        </div>

        {/* Chart B: Time Breakdown */}
        <div className="jobs-chart-card">
          <h3 className="jobs-chart-title">Time Breakdown per Stage</h3>
          {breakdownData.length > 0 ? (
            <ResponsiveContainer width="100%" height={Math.max(200, breakdownData.length * 40)}>
              <BarChart layout="vertical" data={breakdownData} margin={{ top: 5, right: 30, left: 60, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis
                  type="number"
                  tickFormatter={(v) => formatDuration(v)}
                  stroke="var(--text-muted)"
                  fontSize={12}
                />
                <YAxis
                  type="category"
                  dataKey="name"
                  stroke="var(--text-muted)"
                  fontSize={12}
                  width={80}
                />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => formatDuration(v)} />
                <Legend />
                <Bar dataKey="Compute" stackId="a" fill={BREAKDOWN_COLORS.compute} />
                <Bar dataKey="GC" stackId="a" fill={BREAKDOWN_COLORS.gc} />
                <Bar dataKey="Deserialization" stackId="a" fill={BREAKDOWN_COLORS.deserialization} />
                <Bar dataKey="Shuffle Wait" stackId="a" fill={BREAKDOWN_COLORS.shuffleWait} />
                <Bar dataKey="Serialization" stackId="a" fill={BREAKDOWN_COLORS.serialization} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="jobs-empty"><p>No stage data available</p></div>
          )}
        </div>
      </div>

      {/* I/O Table */}
      <div className="jobs-io-section">
        <h3>I/O per Stage</h3>
        <table className="jobs-io-table">
          <thead>
            <tr>
              <th>Stage</th>
              <th>Tasks</th>
              <th>Input</th>
              <th>Input Records</th>
              <th>Output</th>
              <th>Shuffle Read</th>
              <th>Shuffle Write</th>
              <th>Peak Memory</th>
            </tr>
          </thead>
          <tbody>
            {stages.map((s) => (
                <tr key={s.stage_id ?? s.id}>
                  <td>Stage {s.stage_id ?? '?'}</td>
                  <td>{formatNumber(s.num_tasks)}</td>
                  <td>{formatBytes(s.input_bytes)}</td>
                  <td>{formatNumber(s.input_records)}</td>
                  <td>{formatBytes(s.output_bytes)}</td>
                  <td>{formatBytes(s.shuffle_read_bytes)}</td>
                  <td>{formatBytes(s.shuffle_write_bytes)}</td>
                  <td>{formatBytes(s.peak_execution_memory)}</td>
                </tr>
              ))}
            {stages.length === 0 && (
              <tr><td colSpan={8} style={{ textAlign: 'center', color: 'var(--text-muted)' }}>No stage data</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Optimization Signals */}
      <div className="jobs-signals-section">
        <h3>Optimization Signals</h3>
        {signals.length > 0 ? (
          <div className="jobs-signals-grid">
            {signals.map((sig, i) => {
              const IconComp = SIGNAL_ICONS[sig.severity] || Info;
              return (
                <div key={i} className={`jobs-signal-card ${sig.severity}`}>
                  <div className={`jobs-signal-icon ${sig.severity}`}>
                    <IconComp size={20} />
                  </div>
                  <div className="jobs-signal-body">
                    <div className="jobs-signal-header">
                      <span className="jobs-signal-type">{sig.type}</span>
                      <span className={`jobs-signal-severity ${sig.severity}`}>{sig.severity}</span>
                    </div>
                    <div className="jobs-signal-message">{sig.message}</div>
                    <div className="jobs-signal-recommendation">{sig.recommendation}</div>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="jobs-no-signals">
            <ThumbsUp size={32} strokeWidth={1.5} />
            <p>No optimization issues detected for this job</p>
          </div>
        )}
      </div>
    </div>
  );
}

export default Jobs;
