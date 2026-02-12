import { useState, useEffect } from 'react';
import { getDatasets, getSchemaHistory } from '../services/api';
import { FileJson, ChevronDown, Loader2, Database, Plus, Minus, Edit3, AlertTriangle, Check } from 'lucide-react';
import { SchemaTree } from '../components/FieldTree';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './Schema.css';

function Schema() {
  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState('');
  const [schemaHistory, setSchemaHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [selectedVersions, setSelectedVersions] = useState([]);
  const [showDiff, setShowDiff] = useState(false);

  useEffect(() => {
    loadDatasets();
  }, []);

  const loadDatasets = async () => {
    try {
      const res = await getDatasets();
      const datasetList = res.data || [];
      setDatasets(datasetList);
      if (datasetList.length > 0) {
        setSelectedDataset(datasetList[0].name);
        loadSchemaHistory(datasetList[0].name);
      }
    } catch (error) {
      console.error('Failed to load datasets:', error);
    }
    setInitialLoading(false);
  };

  const loadSchemaHistory = async (datasetName) => {
    setLoading(true);
    try {
      const res = await getSchemaHistory(datasetName);
      setSchemaHistory(res.data || []);
      setSelectedVersions([]);
      setShowDiff(false);
    } catch (error) {
      console.error('Failed to load schema history:', error);
      setSchemaHistory([]);
    }
    setLoading(false);
  };

  const toggleVersionSelection = (versionId) => {
    setSelectedVersions(prev => {
      if (prev.includes(versionId)) {
        return prev.filter(id => id !== versionId);
      }
      if (prev.length < 2) {
        return [...prev, versionId];
      }
      // Replace oldest selection
      return [prev[1], versionId];
    });
  };

  const computeDiff = () => {
    if (selectedVersions.length !== 2) return null;

    const v1 = schemaHistory.find(v => v.id === selectedVersions[0]);
    const v2 = schemaHistory.find(v => v.id === selectedVersions[1]);

    if (!v1 || !v2) return null;

    // Determine which is older
    const [older, newer] = new Date(v1.valid_from) < new Date(v2.valid_from)
      ? [v1, v2]
      : [v2, v1];

    const oldFields = {};
    const newFields = {};

    (older.schema_data?.fields || []).forEach(f => {
      oldFields[f.name] = f;
    });

    (newer.schema_data?.fields || []).forEach(f => {
      newFields[f.name] = f;
    });

    const changes = [];
    let hasBreaking = false;

    // Added fields
    Object.keys(newFields).forEach(name => {
      if (!oldFields[name]) {
        changes.push({ type: 'ADDED', field: newFields[name] });
      }
    });

    // Removed fields
    Object.keys(oldFields).forEach(name => {
      if (!newFields[name]) {
        changes.push({ type: 'REMOVED', field: oldFields[name] });
        hasBreaking = true;
      }
    });

    // Modified fields
    Object.keys(oldFields).forEach(name => {
      if (newFields[name]) {
        const oldF = oldFields[name];
        const newF = newFields[name];
        if (oldF.type !== newF.type || oldF.nullable !== newF.nullable) {
          changes.push({
            type: 'MODIFIED',
            field: newF,
            oldField: oldF
          });
          if (oldF.type !== newF.type) hasBreaking = true;
          if (oldF.nullable && !newF.nullable) hasBreaking = true;
        }
      }
    });

    return {
      older,
      newer,
      changes,
      hasBreaking
    };
  };

  const diff = showDiff ? computeDiff() : null;

  const formatDate = (dateStr) => {
    return new Date(dateStr).toLocaleString();
  };

  const MAX_VISIBLE_FIELDS = 10; // Show first 10 top-level fields in tree view

  if (initialLoading) {
    return <LoadingSpinner message="Loading datasets..." />;
  }

  return (
    <div className="schema-page">
      <PageHeader
        title="Schema Evolution"
        description="Track schema changes and compare versions"
        icon={FileJson}
      >
        <div className="selector">
          <label>Dataset</label>
          <div className="select-wrapper">
            <select
              value={selectedDataset}
              onChange={(e) => {
                setSelectedDataset(e.target.value);
                loadSchemaHistory(e.target.value);
              }}
            >
              {datasets.map((d) => (
                <option key={d.id} value={d.name}>
                  {d.name}
                </option>
              ))}
            </select>
            <ChevronDown size={18} className="select-icon" />
          </div>
        </div>
        {selectedVersions.length === 2 && (
          <button
            className={`diff-btn ${showDiff ? 'active' : ''}`}
            onClick={() => setShowDiff(!showDiff)}
          >
            <Edit3 size={16} />
            <span>{showDiff ? 'Hide Diff' : 'Show Diff'}</span>
          </button>
        )}
      </PageHeader>

      {loading ? (
        <div className="loading-content">
          <Loader2 className="loading-spinner" size={32} />
          <span>Loading schema history...</span>
        </div>
      ) : schemaHistory.length === 0 ? (
        <div className="empty-state">
          <FileJson size={48} strokeWidth={1.5} />
          <h3>No schema history</h3>
          <p>No schema versions found for this dataset.</p>
        </div>
      ) : (
        <div className="schema-content">
          {showDiff && diff && (
            <div className="diff-panel">
              <div className="diff-header">
                <div className="diff-title">
                  <Edit3 size={20} />
                  <h3>Schema Diff</h3>
                  {diff.hasBreaking && (
                    <span className="breaking-badge">
                      <AlertTriangle size={14} />
                      Breaking Changes
                    </span>
                  )}
                </div>
                <div className="diff-versions">
                  <span className="version-label">
                    {formatDate(diff.older.valid_from)}
                  </span>
                  <span className="arrow">→</span>
                  <span className="version-label">
                    {formatDate(diff.newer.valid_from)}
                  </span>
                </div>
              </div>

              {diff.changes.length === 0 ? (
                <div className="no-changes">
                  <Check size={24} />
                  <p>No differences found between these versions</p>
                </div>
              ) : (
                <div className="diff-changes">
                  {diff.changes.map((change, idx) => (
                    <div key={idx} className={`change-item ${change.type.toLowerCase()}`}>
                      <div className="change-icon">
                        {change.type === 'ADDED' && <Plus size={16} />}
                        {change.type === 'REMOVED' && <Minus size={16} />}
                        {change.type === 'MODIFIED' && <Edit3 size={16} />}
                      </div>
                      <div className="change-content">
                        <span className="field-name">{change.field.name}</span>
                        <span className="change-type">{change.type}</span>
                        {change.type === 'MODIFIED' ? (
                          <div className="modification">
                            <span className="old-value">
                              {change.oldField.type}
                              {change.oldField.nullable ? ' (nullable)' : ' (required)'}
                            </span>
                            <span className="mod-arrow">→</span>
                            <span className="new-value">
                              {change.field.type}
                              {change.field.nullable ? ' (nullable)' : ' (required)'}
                            </span>
                          </div>
                        ) : (
                          <span className="field-type">
                            {change.field.type}
                            {change.field.nullable ? ' (nullable)' : ' (required)'}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          <div className="versions-section">
            <div className="section-header">
              <h3>Version History</h3>
              <p className="selection-hint">
                {selectedVersions.length === 0 && 'Select two versions to compare'}
                {selectedVersions.length === 1 && 'Select one more version to compare'}
                {selectedVersions.length === 2 && 'Click "Show Diff" to compare'}
              </p>
            </div>

            <div className="versions-list">
              {schemaHistory.map((version, idx) => (
                <div
                  key={version.id}
                  className={`version-card ${selectedVersions.includes(version.id) ? 'selected' : ''} ${version.is_current ? 'current' : ''}`}
                  onClick={() => toggleVersionSelection(version.id)}
                >
                  <div className="version-header">
                    <div className="version-info">
                      <span className="version-number">v{schemaHistory.length - idx}</span>
                      {version.is_current && <span className="current-badge">Current</span>}
                      {selectedVersions.includes(version.id) && (
                        <span className="selected-badge">
                          {selectedVersions.indexOf(version.id) === 0 ? 'From' : 'To'}
                        </span>
                      )}
                    </div>
                    <span className="version-date">{formatDate(version.valid_from)}</span>
                  </div>

                  {version.change_type && (
                    <div className="change-description">
                      <span className="change-badge">{version.change_type}</span>
                      {version.change_description && (
                        <span className="description-text">{version.change_description}</span>
                      )}
                    </div>
                  )}

                  <div className="schema-fields">
                    <div className="fields-header">
                      <Database size={14} />
                      <span>{version.schema_data?.fields?.length || 0} fields</span>
                    </div>
                    <div className="schema-tree-container">
                      <SchemaTree
                        fields={version.schema_data?.fields || []}
                        maxVisible={MAX_VISIBLE_FIELDS}
                      />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default Schema;
