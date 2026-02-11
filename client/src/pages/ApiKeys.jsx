import { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';
import { useToast } from '../context/ToastContext';
import { Key, Copy, Trash2, Plus, Loader2, AlertCircle } from 'lucide-react';
import PageHeader from '../components/PageHeader';
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

  const { token } = useAuth();
  const toast = useToast();

  const fetchKeys = useCallback(async (showLoader = true) => {
    try {
      if (showLoader) {
        setLoading(true);
      }

      // Use explicit API URL from env or default to relative (which handled by proxy in dev, but needs rewrite in prod if not set)
      // Better: use the same API_BASE as AuthContext
      const apiBase = import.meta.env.VITE_API_URL || '';
      const response = await fetch(`${apiBase}/api/v1/auth/api-keys`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        if (response.status === 401) {
          // Token expired or invalid, let AuthContext handle it or redirect
          console.warn('API Keys fetch 401: Unauthorized');
          return;
        }
        throw new Error('Failed to fetch API keys');
      }

      const data = await response.json();
      setKeys(data);
    } catch (err) {
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
  }, [token, toast]);

  useEffect(() => {
    fetchKeys();
  }, []); // Run once on mount

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

    const apiBase = import.meta.env.VITE_API_URL || '';
    try {
      const response = await fetch(`${apiBase}/api/v1/auth/api-keys`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: newKeyName,
          scopes: ['ingest', 'read'], // All keys get both permissions
        }),
      });

      if (!response.ok) {
        const errData = await response.json();
        throw new Error(errData.detail || 'Failed to create API key');
      }

      const data = await response.json();
      setCreatedKey(data);
      setNewKeyName('');
      setNameError('');
      setShowCreateModal(false);
      fetchKeys(false); // Don't show loader when refreshing after creation
      toast.success('API Key Created', 'Your new API key has been generated successfully');
    } catch (err) {
      toast.error('Failed to create key', err.message);
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteKey = async (keyId, keyName) => {
    if (!window.confirm(`Are you sure you want to revoke "${keyName}"? This action cannot be undone.`)) {
      return;
    }

    setDeletingId(keyId);

    const apiBase = import.meta.env.VITE_API_URL || '';
    try {
      const response = await fetch(`${apiBase}/api/v1/auth/api-keys/${keyId}`, {
        method: 'DELETE',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        throw new Error('Failed to revoke API key');
      }

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
                <label>Usage Example:</label>
                <div className="code-block">
                  <pre>{`spark-submit \\
  --conf spark.observability.api.key=${createdKey.key} \\
  your-application.jar`}</pre>
                  <button
                    className="copy-code-btn"
                    onClick={() =>
                      copyToClipboard(
                        `spark-submit \\\n  --conf spark.observability.api.key=${createdKey.key} \\\n  your-application.jar`,
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
