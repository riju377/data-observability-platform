
import React, { useMemo, useState } from 'react';
import './SchemaDiffViewer.css';
import { Plus, Minus, Edit3, AlertCircle, CheckCircle2, ArrowRight, ChevronDown, ChevronUp } from 'lucide-react';

/**
 * Renders a structured schema diff.
 * Accepts either:
 * - diff: Array of structured diff objects (from new backend)
 * - description: String (semicolon separated) for legacy compatibility
 */
const SchemaDiffViewer = ({ diff, description }) => {
    const parsedDiff = useMemo(() => {
        if (diff && Array.isArray(diff)) return diff;
        if (!description) return [];

        // Parse legacy string description
        // Example: "Column added: active_app (double); Type changed: age (int -> string)"
        return description.split(';').map(part => {
            part = part.trim();
            if (!part) return null;

            let action = 'UNKNOWN';
            let column = '';
            let details = '';
            let severity = 'info';

            if (part.startsWith('Column added:')) {
                action = 'COLUMN_ADDED';
                const content = part.replace('Column added:', '').trim();
                const match = content.match(/^(.+?)\s*\((.+)\)$/);
                if (match) {
                    column = match[1];
                    details = `Type: ${match[2]}`;
                } else {
                    column = content;
                }
                severity = 'info';
            } else if (part.startsWith('Column removed:')) {
                action = 'COLUMN_REMOVED';
                const content = part.replace('Column removed:', '').trim();
                const match = content.match(/^(.+?)\s*\(was\s+(.+)\)$/);
                if (match) {
                    column = match[1];
                    details = `Was: ${match[2]}`;
                } else {
                    // Handle simple "Column removed: name"
                    const simpleMatch = content.match(/^(.+?)$/);
                    column = simpleMatch ? simpleMatch[1] : content;
                }
                severity = 'critical';
            } else if (part.startsWith('Type changed:')) {
                action = 'TYPE_CHANGED';
                const content = part.replace('Type changed:', '').trim();
                const match = content.match(/^(.+?)\s*\((.+)\)$/);
                if (match) {
                    column = match[1];
                    details = match[2]; // "int -> string"
                } else {
                    column = content;
                }
                severity = 'warning';
            } else if (part.startsWith('Nullability changed:')) {
                action = 'NULLABILITY_CHANGED';
                const content = part.replace('Nullability changed:', '').trim();
                const match = content.match(/^(.+?)\s*\((.+)\)$/);
                if (match) {
                    column = match[1];
                    details = match[2];
                } else {
                    column = content;
                }
                severity = details.includes('NOT NULL') && !details.includes('-> NULLABLE') ? 'critical' : 'info';
            }

            return { action, column, details, severity };
        }).filter(d => d !== null);
    }, [diff, description]);

    if (!parsedDiff || parsedDiff.length === 0) {
        return <div className="no-diff">No details available</div>;
    }

    const getIcon = (action) => {
        switch (action) {
            case 'COLUMN_ADDED': return <Plus size={14} />;
            case 'COLUMN_REMOVED': return <Minus size={14} />;
            case 'TYPE_CHANGED': return <Edit3 size={14} />;
            case 'NULLABILITY_CHANGED': return <AlertCircle size={14} />;
            default: return <CheckCircle2 size={14} />;
        }
    };

    const getBadgeClass = (action) => {
        switch (action) {
            case 'COLUMN_ADDED': return 'badge-added';
            case 'COLUMN_REMOVED': return 'badge-removed';
            case 'TYPE_CHANGED': return 'badge-changed';
            default: return 'badge-info';
        }
    };

    const formatAction = (action) => {
        if (!action) return 'CHANGE';
        return action.replace('COLUMN_', '').replace('_CHANGED', '').replace('NULLABILITY', 'NULL');
    };

    const [expanded, setExpanded] = useState(false);
    const INITIAL_LIMIT = 5;
    const hasMore = parsedDiff.length > INITIAL_LIMIT;
    const displayedDiff = expanded ? parsedDiff : parsedDiff.slice(0, INITIAL_LIMIT);

    return (
        <div className="schema-diff-viewer">
            <table className="diff-table">
                <thead>
                    <tr>
                        <th className="col-action">Action</th>
                        <th className="col-name">Column</th>
                        <th className="col-details">Details</th>
                    </tr>
                </thead>
                <tbody>
                    {displayedDiff.map((item, idx) => (
                        <tr key={idx}>
                            <td className="col-action">
                                <span className={`diff-badge ${getBadgeClass(item.action)}`}>
                                    {getIcon(item.action)}
                                    <span>{formatAction(item.action)}</span>
                                </span>
                            </td>
                            <td className="col-name">{item.column}</td>
                            <td className="col-details">
                                {item.details && item.details.includes('->') ? (
                                    <span className="diff-arrow-text">
                                        {item.details.split('->')[0]}
                                        <ArrowRight size={12} className="inline-arrow" />
                                        {item.details.split('->')[1]}
                                    </span>
                                ) : (
                                    item.details
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
            {hasMore && (
                <button
                    className="diff-expand-btn"
                    onClick={() => setExpanded(!expanded)}
                >
                    {expanded ? (
                        <>
                            <ChevronUp size={14} />
                            <span>Show less</span>
                        </>
                    ) : (
                        <>
                            <ChevronDown size={14} />
                            <span>Show {parsedDiff.length - INITIAL_LIMIT} more changes</span>
                        </>
                    )}
                </button>
            )}
        </div>
    );
};

export default SchemaDiffViewer;
