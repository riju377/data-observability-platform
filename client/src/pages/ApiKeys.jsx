import { useState, useEffect, useCallback } from 'react';
import { useToast } from '../context/ToastContext';
import { Key, Copy, Trash2, Plus, Loader2, AlertCircle, Terminal, BookOpen, ChevronRight } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { getApiKeys, createApiKey, deleteApiKey } from '../services/api';
import './ApiKeys.css';

function ApiKeys() {
  const [keys, setKeys] = useState([]);
  const [loading, setLoading] = useState(true);
  const [initialLoading, setInitialLoading] = useState(true);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newKeyName, setNewKeyName] = useState('');
  const [nameError, setNameError] = useState('');
  const [createdKey, setCreatedKey] = useState(null);
  const [creating, setCreating] = useState(false);
  const [copiedId, setCopiedId] = useState(null);
  const [deletingId, setDeletingId] = useState(null);
  const [activeTab, setActiveTab] = useState('spark-submit');

  const toast = useToast();

  const fetchKeys = useCallback(async (showLoader = true) => {
    try {
      if (showLoader) {
        setLoading(true);
      }

      const res = await getApiKeys();
      setKeys(res.data);
    } catch (err) {
      // 401 handled by interceptor automatically
      console.error('API Keys Error:', err);
      // Only show toast if it's NOT a "Unexpected token <" html error which spams
      if (!err.message.includes('Unexpected token')) {
        toast.error('Failed to load API keys', err.message);
      }
    } finally {
      if (showLoader) {
        setLoading(false);
      }
      setInitialLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    fetchKeys();
    fetchLatestVersion();
  }, []); // Run once on mount

  // Fetch latest version from Maven Central
  const [latestVersion, setLatestVersion] = useState('1.3.0'); // Fallback default

  const fetchLatestVersion = async () => {
    try {
      // Fetch maven-metadata.xml via public CORS proxy (allorigins.win)
      const metadataUrl =
        "https://repo1.maven.org/maven2/io/github/riju377/data-observability-platform_2.12/maven-metadata.xml";
      const proxyUrl = "https://api.allorigins.win/raw?url=" + encodeURIComponent(metadataUrl);

      const response = await fetch(proxyUrl);
      if (response.ok) {
        const text = await response.text();
        const parser = new DOMParser();
        const xml = parser.parseFromString(text, "application/xml");
        const latest = xml.querySelector("latest")?.textContent;

        if (latest) {
          setLatestVersion(latest);
        }
      }
    } catch (e) {
      console.warn('Failed to fetch latest version via allorigins', e);
    }
  };

  // Real-time validation
  const validateKeyName = (name) => {
    if (!name.trim()) {
      setNameError('Key name is required');
      return false;
    }
    if (name.length < 3) {
      setNameError('Key name must be at least 3 characters');
      return false;
    }
    if (name.length > 50) {
      setNameError('Key name must be less than 50 characters');
      return false;
    }
    setNameError('');
    return true;
  };

  const handleNameChange = (e) => {
    const value = e.target.value;
    setNewKeyName(value);
    if (value) {
      validateKeyName(value);
    } else {
      setNameError('');
    }
  };

  const handleCreateKey = async (e) => {
    e.preventDefault();
    if (!validateKeyName(newKeyName)) return;

    setCreating(true);

    try {
      const res = await createApiKey({
        name: newKeyName,
        scopes: ['ingest', 'read'], // All keys get both permissions
      });

      setCreatedKey(res.data);
      setNewKeyName('');
      setNameError('');
      setShowCreateModal(false);
      fetchKeys(false); // Don't show loader when refreshing after creation
      toast.success('API Key Created', 'Your new API key has been generated successfully');
    } catch (err) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to create API key';
      toast.error('Failed to create key', errorMessage);
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteKey = async (keyId, keyName) => {
    if (!window.confirm(`Are you sure you want to revoke "${keyName}"? This action cannot be undone.`)) {
      return;
    }

    setDeletingId(keyId);

    try {
      await deleteApiKey(keyId);

      fetchKeys(false); // Don't show loader when refreshing after deletion
      toast.success('API Key Revoked', `"${keyName}" has been permanently revoked`);
    } catch (err) {
      toast.error('Failed to revoke key', err.message);
    } finally {
      setDeletingId(null);
    }
  };

  const copyToClipboard = async (text, id, label = 'API key') => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedId(id);
      setTimeout(() => setCopiedId(null), 2000);
      toast.success('Copied!', `${label} copied to clipboard`);
    } catch (err) {
      toast.error('Copy failed', 'Could not copy to clipboard');
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return 'Never';
    return new Date(dateStr).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const closeCreatedKeyModal = () => {
    if (!copiedId) {
      const confirmed = window.confirm(
        "Have you copied your API key? You won't be able to see it again!"
      );
      if (!confirmed) return;
    }
    setCreatedKey(null);
  };

  if (initialLoading) {
    return (
      <div className="api-keys-container">
        <div className="loading-container">
          <Loader2 size={48} className="loading-spinner" />
          <p>Loading API keys...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="api-keys-container">
      {/* Header */}
      <PageHeader
        title="API Keys"
        description="Manage API keys for Spark job integration"
        icon={Key}
      >
        <button className="btn-primary" onClick={() => setShowCreateModal(true)}>
          <Plus size={18} />
          Create API Key
        </button>
      </PageHeader>

      {/* Success Modal - Shows created key once */}
      {createdKey && (
        <div className="modal-overlay" onClick={closeCreatedKeyModal} aria-labelledby="modal-title">
          <div className="modal modal-success" onClick={(e) => e.stopPropagation()} role="dialog">
            <div className="modal-header">
              <h2 id="modal-title">API Key Created Successfully</h2>
            </div>
            <div className="modal-body">
              <div className="alert alert-warning">
                <AlertCircle size={20} />
                <div>
                  <strong>Important:</strong> Copy this key now. You won't be able to see it again!
                </div>
              </div>

              <div className="form-group">
                <label>Your API Key:</label>
                <div className="key-display-container">
                  <code className="key-display">{createdKey.key}</code>
                  <button
                    className="btn-icon"
                    onClick={() => copyToClipboard(createdKey.key, 'new', 'API key')}
                    title="Copy to clipboard"
                  >
                    {copiedId === 'new' ? '✓' : <Copy size={16} />}
                  </button>
                </div>
              </div>

              <div className="form-group">
                <label>Quick Start:</label>
                <div className="code-block">
                  <pre>{`spark-submit \\
  --packages io.github.riju377:data-observability-platform_2.12:${latestVersion} \\
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.observability.api.key=${createdKey.key} \\
  your-application.jar`}</pre>
                  <button
                    className="copy-code-btn"
                    onClick={() =>
                      copyToClipboard(
                        `spark-submit \\
  --packages io.github.riju377:data-observability-platform_2.12:${latestVersion} \\
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.observability.api.key=${createdKey.key} \\
  your-application.jar`,
                        'example',
                        'Example code'
                      )
                    }
                  >
                    {copiedId === 'example' ? '✓ Copied' : 'Copy'}
                  </button>
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn-primary" onClick={() => setCreatedKey(null)}>
                I've Saved the Key
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Create Key Modal */}
      {showCreateModal && (
        <div
          className="modal-overlay"
          onClick={() => {
            setShowCreateModal(false);
            setNewKeyName('');
            setNameError('');
          }}
          aria-labelledby="create-modal-title"
        >
          <div className="modal" onClick={(e) => e.stopPropagation()} role="dialog">
            <div className="modal-header">
              <h2 id="create-modal-title">Create New API Key</h2>
            </div>
            <form onSubmit={handleCreateKey}>
              <div className="modal-body">
                <div className="form-group">
                  <label htmlFor="keyName">
                    Key Name <span className="required">*</span>
                  </label>
                  <input
                    id="keyName"
                    type="text"
                    value={newKeyName}
                    onChange={handleNameChange}
                    placeholder="e.g., Production ETL, Dev Environment"
                    className={nameError ? 'input-error' : ''}
                    autoFocus
                    required
                    maxLength={50}
                  />
                  {nameError ? (
                    <span className="error-text">{nameError}</span>
                  ) : (
                    <span className="hint-text">
                      A descriptive name to identify this key (3-50 characters)
                    </span>
                  )}
                  <div className="char-count">
                    {newKeyName.length}/50
                  </div>
                </div>

                <div className="info-note-modal">
                  <AlertCircle size={16} />
                  <span>
                    This key will have both <strong>Ingest</strong> and <strong>Read</strong> permissions.
                  </span>
                </div>
              </div>
              <div className="modal-footer">
                <button
                  type="button"
                  className="btn-secondary"
                  onClick={() => {
                    setShowCreateModal(false);
                    setNewKeyName('');
                    setNameError('');
                  }}
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="btn-primary"
                  disabled={creating || !newKeyName.trim() || nameError}
                >
                  {creating ? (
                    <>
                      <Loader2 size={16} className="spinner" />
                      Creating...
                    </>
                  ) : (
                    <>
                      <Plus size={16} />
                      Create Key
                    </>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Keys List */}
      <div className="keys-list-container">
        {keys.length === 0 ? (
          <div className="empty-state">
            <Key size={64} className="empty-icon" />
            <h3>No API Keys Yet</h3>
            <p>Create an API key to start sending metrics from your Spark jobs.</p>
            <button className="btn-primary" onClick={() => setShowCreateModal(true)}>
              <Plus size={18} />
              Create Your First Key
            </button>
          </div>
        ) : (
          <div className="table-container">
            <table className="keys-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Key Prefix</th>
                  <th>Scopes</th>
                  <th>Created</th>
                  <th>Expires</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {keys.map((key) => (
                  <tr key={key.id}>
                    <td className="key-name">
                      <Key size={16} className="key-icon" />
                      {key.name}
                    </td>
                    <td className="key-prefix">
                      <code>{key.key_prefix}...</code>
                    </td>
                    <td className="scopes">
                      {key.scopes.map((scope) => (
                        <span key={scope} className="badge badge-scope">
                          {scope}
                        </span>
                      ))}
                    </td>
                    <td className="date">{formatDate(key.created_at)}</td>
                    <td className="date">
                      {key.expires_at ? formatDate(key.expires_at) : (
                        <span className="badge badge-never">Never</span>
                      )}
                    </td>
                    <td className="actions">
                      <button
                        className="btn-danger"
                        onClick={() => handleDeleteKey(key.id, key.name)}
                        disabled={deletingId === key.id}
                        title="Revoke this key"
                      >
                        {deletingId === key.id ? (
                          <Loader2 size={16} className="spinner" />
                        ) : (
                          <Trash2 size={16} />
                        )}
                        Revoke
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Integration Guide */}
      <div className="integration-guide">
        <div className="guide-header">
          <div className="guide-header-left">
            <BookOpen size={22} />
            <div>
              <h2>Integration Guide</h2>
              <p>Add observability to your Spark jobs in 3 steps</p>
            </div>
          </div>
        </div>

        <div className="guide-steps">
          {/* Step 1 */}
          <div className="guide-step">
            <div className="step-number">1</div>
            <div className="step-content">
              <h3>Add the Package</h3>
              <p>
                Choose your build tool and add the Data Observability listener.
                <a
                  href="https://central.sonatype.com/artifact/io.github.riju377/data-observability-platform_2.12"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="version-link"
                  style={{ marginLeft: '10px', fontSize: '0.8rem', color: '#667eea', textDecoration: 'none' }}
                >
                  Latest: {latestVersion} ↗
                </a>
              </p>

              <div className="code-tabs">
                <div className="tab-bar">
                  <button
                    className={`tab-btn ${activeTab === 'spark-submit' ? 'active' : ''}`}
                    onClick={() => setActiveTab('spark-submit')}
                  >
                    <Terminal size={14} />
                    spark-submit
                  </button>
                  <button
                    className={`tab-btn ${activeTab === 'sbt' ? 'active' : ''}`}
                    onClick={() => setActiveTab('sbt')}
                  >
                    SBT
                  </button>
                  <button
                    className={`tab-btn ${activeTab === 'maven' ? 'active' : ''}`}
                    onClick={() => setActiveTab('maven')}
                  >
                    Maven
                  </button>
                </div>

                {activeTab === 'spark-submit' && (
                  <div className="code-block">
                    <pre>{`spark-submit \\
  --packages io.github.riju377:data-observability-platform_2.12:${latestVersion} \\
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.observability.api.key=<YOUR_API_KEY> \\
  your-application.jar`}</pre>
                    <button
                      className="copy-code-btn"
                      onClick={() =>
                        copyToClipboard(
                          `spark-submit \\
  --packages io.github.riju377:data-observability-platform_2.12:${latestVersion} \\
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.observability.api.key=${keys.length > 0 ? keys[0].key_prefix + '...' : '<YOUR_API_KEY>'} \\
  your-application.jar`,
                          'guide-spark',
                          'spark-submit command'
                        )
                      }
                    >
                      {copiedId === 'guide-spark' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                )}

                {activeTab === 'sbt' && (
                  <div className="code-block">
                    <pre>{`// build.sbt
libraryDependencies += "io.github.riju377" %% "data-observability-platform" % "${latestVersion}"`}</pre>
                    <button
                      className="copy-code-btn"
                      onClick={() =>
                        copyToClipboard(
                          `libraryDependencies += "io.github.riju377" %% "data-observability-platform" % "${latestVersion}"`,
                          'guide-sbt',
                          'SBT dependency'
                        )
                      }
                    >
                      {copiedId === 'guide-sbt' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                )}

                {activeTab === 'maven' && (
                  <div className="code-block">
                    <pre>{`<dependency>
  <groupId>io.github.riju377</groupId>
  <artifactId>data-observability-platform_2.12</artifactId>
  <version>${latestVersion}</version>
</dependency>`}</pre>
                    <button
                      className="copy-code-btn"
                      onClick={() =>
                        copyToClipboard(
                          `<dependency>\n  <groupId>io.github.riju377</groupId>\n  <artifactId>data-observability-platform_2.12</artifactId>\n  <version>${latestVersion}</version>\n</dependency>`,
                          'guide-maven',
                          'Maven dependency'
                        )
                      }
                    >
                      {copiedId === 'guide-maven' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Step 2 */}
          <div className="guide-step">
            <div className="step-number">2</div>
            <div className="step-content">
              <h3>Configure the Listener</h3>
              <p>Pass these Spark configuration flags when submitting your job:</p>
              <div className="config-table">
                <div className="config-row">
                  <code className="config-key">spark.extraListeners</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">com.observability.listener.ObservabilityListener</code>
                </div>
                <div className="config-row">
                  <code className="config-key">spark.observability.api.key</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">{keys.length > 0 ? keys[0].key_prefix + '...' : '<YOUR_API_KEY>'}</code>
                </div>
                <div className="config-row config-row-optional">
                  <code className="config-key">spark.observability.api.url</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">Optional — defaults to production API</code>
                </div>
              </div>
            </div>
          </div>

          {/* Step 3 */}
          <div className="guide-step">
            <div className="step-number">3</div>
            <div className="step-content">
              <h3>Run &amp; Monitor</h3>
              <p>Submit your Spark job as usual. The listener automatically captures:</p>
              <div className="captures-grid">
                <div className="capture-item">
                  <span className="capture-icon">📊</span>
                  <span>Dataset metrics &amp; row counts</span>
                </div>
                <div className="capture-item">
                  <span className="capture-icon">🔗</span>
                  <span>Table &amp; column lineage</span>
                </div>
                <div className="capture-item">
                  <span className="capture-icon">📋</span>
                  <span>Schema evolution tracking</span>
                </div>
                <div className="capture-item">
                  <span className="capture-icon">🚨</span>
                  <span>Anomaly detection &amp; alerts</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Info Footer */}
      <div className="info-footer">
        <AlertCircle size={16} />
        <span>
          <strong>Coming Soon:</strong> Usage tracking will show when each key was last used.
        </span>
      </div>
    </div>
  );
}

export default ApiKeys;
