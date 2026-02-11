import { useState, useEffect } from 'react';
import { getAlertRules, createAlertRule, updateAlertRule, deleteAlertRule, getAlertHistory, getDatasets } from '../services/api';
import { Bell, Plus, X, Trash2, CheckCircle, XCircle, Mail, AlertTriangle, Loader2, Clock, Send, ShieldAlert, History, List, ChevronDown, Hash, MessageCircle, Pencil, Database, Filter, Globe } from 'lucide-react';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './Alerts.css';

const INITIAL_FORM = {
  name: '',
  description: '',
  severity: '',
  anomaly_type: 'ALL',
  dataset_name: 'ALL',
  channel_type: 'EMAIL',
  channel_config: {},
  email_to: '',
  email_subject_template: '',
  slack_webhook: '',
  slack_channel: '',
  teams_webhook: '',
  enabled: true,
};

function Alerts() {
  const [rules, setRules] = useState([]);
  const [history, setHistory] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [activeTab, setActiveTab] = useState('rules');
  const [formData, setFormData] = useState({ ...INITIAL_FORM });

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setLoading(true);
    try {
      const [rulesRes, historyRes, datasetsRes] = await Promise.all([
        getAlertRules(),
        getAlertHistory(720),
        getDatasets(),
      ]);
      setRules(rulesRes.data || []);
      setHistory(historyRes.data || []);
      setDatasets(datasetsRes.data || []);
    } catch (error) {
      console.error('Failed to load alert data:', error);
      setRules([]);
      setHistory([]);
    }
    setLoading(false);
  };

  const openCreateForm = () => {
    setEditingRule(null);
    setFormData({ ...INITIAL_FORM });
    setShowForm(true);
  };

  const openEditForm = (rule) => {
    setEditingRule(rule);
    const config = rule.channel_config || {};
    setFormData({
      name: rule.name || '',
      description: rule.description || '',
      severity: rule.severity || '',
      anomaly_type: rule.anomaly_type || 'ALL',
      dataset_name: rule.dataset_name || 'ALL',
      channel_type: rule.channel_type || 'EMAIL',
      channel_config: config,
      email_to: config.email_to?.join(', ') || '',
      email_subject_template: config.subject_template || '',
      slack_webhook: config.webhook_url || '',
      slack_channel: config.channel || '',
      teams_webhook: config.webhook_url || '',
      enabled: rule.enabled !== false,
    });
    setShowForm(true);
  };

  const closeForm = () => {
    setShowForm(false);
    setEditingRule(null);
    setFormData({ ...INITIAL_FORM });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    try {
      const payload = {
        name: formData.name,
        description: formData.description,
        severity: formData.severity || null,
        anomaly_type: formData.anomaly_type === 'ALL' ? null : formData.anomaly_type,
        dataset_name: formData.dataset_name === 'ALL' ? null : formData.dataset_name,
        enabled: formData.enabled,
        channel_type: formData.channel_type,
        channel_config: {},
      };

      if (formData.channel_type === 'EMAIL') {
        payload.channel_config = {
          email_to: formData.email_to.split(',').map(e => e.trim()).filter(Boolean),
          subject_template: formData.email_subject_template || null,
        };
      } else if (formData.channel_type === 'SLACK') {
        payload.channel_config = {
          webhook_url: formData.slack_webhook,
          channel: formData.slack_channel || null,
        };
      } else if (formData.channel_type === 'TEAMS') {
        payload.channel_config = {
          webhook_url: formData.teams_webhook,
        };
      }

      if (editingRule) {
        await updateAlertRule(editingRule.id, payload);
      } else {
        await createAlertRule(payload);
      }

      closeForm();
      loadData();
    } catch (error) {
      console.error('Failed to save alert rule:', error);
    }
    setSubmitting(false);
  };

  const handleDelete = async (id) => {
    if (!confirm('Delete this alert rule?')) return;
    try {
      await deleteAlertRule(id);
      loadData();
    } catch (error) {
      console.error('Failed to delete alert rule:', error);
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now - d;
    const diffH = Math.floor(diffMs / 3600000);
    const diffD = Math.floor(diffH / 24);
    if (diffH < 1) return 'Just now';
    if (diffH < 24) return `${diffH}h ago`;
    if (diffD < 7) return `${diffD}d ago`;
    return d.toLocaleDateString();
  };

  const formatFullDate = (dateStr) => {
    if (!dateStr) return '';
    return new Date(dateStr).toLocaleString();
  };

  const formatAnomalyType = (type) => {
    if (!type) return 'All Types';
    return type.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  };

  const sentCount = history.filter(h => h.status === 'SENT').length;
  const failedCount = history.filter(h => h.status === 'FAILED').length;
  const activeRules = rules.filter(r => r.enabled).length;

  if (loading) {
    return <LoadingSpinner message="Loading alerts..." />;
  }

  return (
    <div className="alerts">
      <PageHeader
        title="Alerts"
        description="Configure notifications and view alert history"
        icon={Bell}
      >
        <button className="btn-primary" onClick={() => showForm ? closeForm() : openCreateForm()}>
          {showForm ? (
            <><X size={18} /><span>Cancel</span></>
          ) : (
            <><Plus size={18} /><span>New Rule</span></>
          )}
        </button>
      </PageHeader>

      {/* Stats */}
      <div className="alert-stats">
        <div className="alert-stat-card">
          <div className="alert-stat-icon rules-icon">
            <ShieldAlert size={22} />
          </div>
          <div className="alert-stat-body">
            <div className="alert-stat-value">{activeRules}</div>
            <div className="alert-stat-label">Active Rules</div>
          </div>
        </div>
        <div className="alert-stat-card">
          <div className="alert-stat-icon sent-icon">
            <Send size={22} />
          </div>
          <div className="alert-stat-body">
            <div className="alert-stat-value">{sentCount}</div>
            <div className="alert-stat-label">Alerts Sent</div>
          </div>
        </div>
        <div className="alert-stat-card">
          <div className="alert-stat-icon failed-icon">
            <XCircle size={22} />
          </div>
          <div className="alert-stat-body">
            <div className="alert-stat-value">{failedCount}</div>
            <div className="alert-stat-label">Failed Deliveries</div>
          </div>
        </div>
        <div className="alert-stat-card">
          <div className="alert-stat-icon total-icon">
            <History size={22} />
          </div>
          <div className="alert-stat-body">
            <div className="alert-stat-value">{history.length}</div>
            <div className="alert-stat-label">Total (30d)</div>
          </div>
        </div>
      </div>

      {/* Create / Edit Form */}
      {showForm && (
        <form className="alert-form" onSubmit={handleSubmit}>
          <h3>{editingRule ? 'Edit Alert Rule' : 'Create New Alert Rule'}</h3>
          <div className="form-group">
            <label>Rule Name</label>
            <input
              type="text"
              placeholder="e.g., Critical Anomaly Alert"
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              required
            />
          </div>
          <div className="form-group">
            <label>Description</label>
            <textarea
              placeholder="Describe what this rule monitors..."
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
            />
          </div>

          <div className="form-row">
            <div className="form-group">
              <label>Target Dataset</label>
              <div className="select-wrapper">
                <select
                  value={formData.dataset_name}
                  onChange={(e) => setFormData({ ...formData, dataset_name: e.target.value })}
                >
                  <option value="ALL">All Datasets (Global)</option>
                  {datasets.map(d => (
                    <option key={d.name} value={d.name}>{d.name}</option>
                  ))}
                </select>
                <ChevronDown size={16} className="select-icon" />
              </div>
            </div>

            <div className="form-group">
              <label>Anomaly Type</label>
              <div className="select-wrapper">
                <select
                  value={formData.anomaly_type}
                  onChange={(e) => setFormData({ ...formData, anomaly_type: e.target.value })}
                >
                  <option value="ALL">All Types</option>
                  <option value="ROW_COUNT_DROP">Row Count Drop</option>
                  <option value="ROW_COUNT_SPIKE">Row Count Spike</option>
                  <option value="SCHEMA_CHANGE">Schema Change</option>
                  <option value="FRESHNESS_VIOLATION">Data Freshness</option>
                  <option value="SizeAnomaly">Volume Anomaly</option>
                </select>
                <ChevronDown size={16} className="select-icon" />
              </div>
            </div>
          </div>
          <div className="form-row">
            <div className="form-group">
              <label>Severity Filter</label>
              <div className="select-wrapper">
                <select
                  value={formData.severity}
                  onChange={(e) => setFormData({ ...formData, severity: e.target.value })}
                >
                  <option value="">All Severities</option>
                  <option value="CRITICAL">Critical</option>
                  <option value="WARNING">Warning</option>
                  <option value="INFO">Info</option>
                </select>
                <ChevronDown size={16} className="select-icon" />
              </div>
            </div>
            <div className="form-group">
              <label>Status</label>
              <div className="toggle-group">
                <label className="toggle">
                  <input
                    type="checkbox"
                    checked={formData.enabled}
                    onChange={(e) => setFormData({ ...formData, enabled: e.target.checked })}
                  />
                  <span className="toggle-slider"></span>
                </label>
                <span className="toggle-label">{formData.enabled ? 'Enabled' : 'Disabled'}</span>
              </div>
            </div>
          </div>
          <div className="form-group">
            <label>Channel Type</label>
            <div className="channel-selector">
              <button
                type="button"
                className={`channel-btn ${formData.channel_type === 'EMAIL' ? 'active' : ''}`}
                onClick={() => setFormData({ ...formData, channel_type: 'EMAIL' })}
              >
                <Mail size={18} /> Email
              </button>
              <button
                type="button"
                className={`channel-btn ${formData.channel_type === 'SLACK' ? 'active' : ''}`}
                onClick={() => setFormData({ ...formData, channel_type: 'SLACK' })}
              >
                <Hash size={18} /> Slack
              </button>
              <button
                type="button"
                className={`channel-btn ${formData.channel_type === 'TEAMS' ? 'active' : ''}`}
                onClick={() => setFormData({ ...formData, channel_type: 'TEAMS' })}
              >
                <MessageCircle size={18} /> Teams
              </button>
            </div>
          </div>

          {formData.channel_type === 'EMAIL' && (
            <>
              <div className="form-group">
                <label><Mail size={16} /> Recipients</label>
                <input
                  type="text"
                  placeholder="email1@example.com, email2@example.com"
                  value={formData.email_to}
                  onChange={(e) => setFormData({ ...formData, email_to: e.target.value })}
                  required
                />
                <span className="form-hint">Separate multiple emails with commas</span>
              </div>
              <div className="form-group">
                <label>Subject Template (optional)</label>
                <input
                  type="text"
                  placeholder="e.g., [ALERT] {severity}: {anomaly_type}"
                  value={formData.email_subject_template}
                  onChange={(e) => setFormData({ ...formData, email_subject_template: e.target.value })}
                />
              </div>
            </>
          )}

          {formData.channel_type === 'SLACK' && (
            <>
              <div className="form-group">
                <label>Webhook URL</label>
                <input
                  type="text"
                  placeholder="https://hooks.slack.com/services/..."
                  value={formData.slack_webhook}
                  onChange={(e) => setFormData({ ...formData, slack_webhook: e.target.value })}
                  required
                />
              </div>
              <div className="form-group">
                <label>Channel Name (optional)</label>
                <input
                  type="text"
                  placeholder="#alerts"
                  value={formData.slack_channel}
                  onChange={(e) => setFormData({ ...formData, slack_channel: e.target.value })}
                />
              </div>
            </>
          )}

          {formData.channel_type === 'TEAMS' && (
            <div className="form-group">
              <label>Webhook URL</label>
              <input
                type="text"
                placeholder="https://outlook.office.com/webhook/..."
                value={formData.teams_webhook}
                onChange={(e) => setFormData({ ...formData, teams_webhook: e.target.value })}
                required
              />
            </div>
          )}

          <div className="form-actions">
            <button type="button" className="btn-secondary" onClick={closeForm}>Cancel</button>
            <button type="submit" className="btn-primary" disabled={submitting}>
              {submitting ? (
                <><Loader2 className="loading-spinner" size={16} /><span>{editingRule ? 'Saving...' : 'Creating...'}</span></>
              ) : (
                <><CheckCircle size={16} /><span>{editingRule ? 'Save Changes' : 'Create Rule'}</span></>
              )}
            </button>
          </div>
        </form>
      )}

      {/* Tabs */}
      <div className="alert-tabs">
        <button
          className={`tab-btn ${activeTab === 'rules' ? 'active' : ''}`}
          onClick={() => setActiveTab('rules')}
        >
          <List size={16} />
          <span>Rules ({rules.length})</span>
        </button>
        <button
          className={`tab-btn ${activeTab === 'history' ? 'active' : ''}`}
          onClick={() => setActiveTab('history')}
        >
          <History size={16} />
          <span>History ({history.length})</span>
        </button>
      </div>

      {/* Rules Tab */}
      {activeTab === 'rules' && (
        <>
          {rules.length === 0 ? (
            <div className="empty-state">
              <Bell size={48} strokeWidth={1.5} />
              <h3>No alert rules yet</h3>
              <p>Create your first alert rule to get notified about anomalies.</p>
              <button className="btn-primary" onClick={openCreateForm}>
                <Plus size={18} /><span>Create Alert Rule</span>
              </button>
            </div>
          ) : (
            <div className="rules-list">
              {rules.map((rule) => (
                <div key={rule.id} className={`rule-card ${rule.enabled ? '' : 'disabled'}`}>
                  <div className="rule-header">
                    <div className="rule-title">
                      <div className={`rule-icon-wrapper ${rule.enabled ? 'active' : 'inactive'}`}>
                        <Bell size={16} />
                      </div>
                      <div>
                        <h3>{rule.name}</h3>
                        {rule.description && <p className="rule-description">{rule.description}</p>}
                      </div>
                    </div>
                    <div className="rule-badges">
                      {rule.severity ? (
                        <span className={`severity-pill ${rule.severity.toLowerCase()}`}>
                          {rule.severity}
                        </span>
                      ) : (
                        <span className="severity-pill all">ALL</span>
                      )}
                      <span className={`status-badge ${rule.enabled ? 'enabled' : 'disabled'}`}>
                        {rule.enabled ? (
                          <><CheckCircle size={13} /> Active</>
                        ) : (
                          <><XCircle size={13} /> Disabled</>
                        )}
                      </span>
                    </div>
                  </div>

                  <div className="rule-details-grid">
                    <div className="rule-detail">
                      <Database size={14} />
                      <span className="detail-label">Dataset</span>
                      <span className="detail-value">
                        {rule.dataset_name || (
                          <span className="detail-global"><Globe size={12} /> All Datasets</span>
                        )}
                      </span>
                    </div>
                    <div className="rule-detail">
                      <Filter size={14} />
                      <span className="detail-label">Type</span>
                      <span className="detail-value">{formatAnomalyType(rule.anomaly_type)}</span>
                    </div>
                    <div className="rule-detail">
                      {rule.channel_type === 'EMAIL' ? <Mail size={14} /> :
                        rule.channel_type === 'SLACK' ? <Hash size={14} /> :
                          <MessageCircle size={14} />}
                      <span className="detail-label">Channel</span>
                      <span className="detail-value">
                        {rule.channel_type === 'EMAIL' ? (
                          <span className="email-chips">
                            {rule.channel_config?.email_to?.map((email, i) => (
                              <span key={i} className="email-chip">{email}</span>
                            ))}
                          </span>
                        ) : rule.channel_type === 'SLACK' ? (
                          'Slack Webhook'
                        ) : (
                          'Teams Webhook'
                        )}
                      </span>
                    </div>
                  </div>

                  <div className="rule-footer">
                    <button className="btn-edit-sm" onClick={() => openEditForm(rule)}>
                      <Pencil size={14} />
                      <span>Edit</span>
                    </button>
                    <button className="btn-danger-sm" onClick={() => handleDelete(rule.id)}>
                      <Trash2 size={14} />
                      <span>Delete</span>
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {/* History Tab */}
      {activeTab === 'history' && (
        <>
          {history.length === 0 ? (
            <div className="empty-state">
              <History size={48} strokeWidth={1.5} />
              <h3>No alert history</h3>
              <p>Alerts will appear here once anomalies trigger notification rules.</p>
            </div>
          ) : (
            <div className="history-list">
              {history.map((item) => (
                <div key={item.id} className={`history-card ${item.status.toLowerCase()}`}>
                  <div className="history-status-indicator">
                    {item.status === 'SENT' ? (
                      <CheckCircle size={20} className="status-sent" />
                    ) : item.status === 'FAILED' ? (
                      <XCircle size={20} className="status-failed" />
                    ) : (
                      <Clock size={20} className="status-dedup" />
                    )}
                  </div>
                  <div className="history-body">
                    <div className="history-subject">
                      {item.channel_type}: {item.status} ({item.anomaly_id.substring(0, 8)})
                    </div>
                    <div className="history-details">
                      <span className={`history-status-tag ${item.status.toLowerCase()}`}>
                        {item.status}
                      </span>
                      <span className="history-recipients">
                        {item.channel_type === 'EMAIL' ? <Mail size={12} /> :
                          item.channel_type === 'SLACK' ? <Hash size={12} /> :
                            <MessageCircle size={12} />}
                        {item.channel_type}
                      </span>
                    </div>
                    {item.error_message && (
                      <div className="history-error">
                        <AlertTriangle size={12} />
                        {item.error_message}
                      </div>
                    )}
                  </div>
                  <div className="history-time">
                    <span className="time-relative">{formatDate(item.sent_at)}</span>
                    <span className="time-full">{formatFullDate(item.sent_at)}</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default Alerts;
