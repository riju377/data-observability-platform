import { useState, useRef, useEffect } from 'react';
import { ChevronRight, ChevronDown, Circle, Box, List, Hash, Type, Calendar, ToggleLeft, Copy, Check } from 'lucide-react';
import { parseSparkType, formatFieldForDisplay, getFieldColor } from '../utils/schemaParser';
import './FieldTree.css';

/**
 * FieldTree Component
 *
 * Recursively renders schema fields in a hierarchical tree structure
 * Handles nested structs, arrays, and maps with expand/collapse
 *
 * @param {object} field - Parsed field object from schemaParser
 * @param {number} level - Nesting level (0 = root, 1 = first level, etc.)
 * @param {boolean} nullable - Whether the field is nullable
 * @param {string} parentPath - Full path from root to this field
 */
function FieldTree({ field, level = 0, nullable = true, isLast = false, parentPath = '' }) {
  const [isExpanded, setIsExpanded] = useState(level === 0); // Auto-expand root level
  const [showTooltip, setShowTooltip] = useState(false);
  const [copied, setCopied] = useState(false);
  const nameRef = useRef(null);
  const [isTruncated, setIsTruncated] = useState(false);

  // Full path for this field (used for copy feature)
  const fullPath = parentPath ? `${parentPath}.${field.name}` : field.name;

  // Check if text is truncated
  useEffect(() => {
    if (nameRef.current) {
      setIsTruncated(nameRef.current.scrollWidth > nameRef.current.clientWidth);
    }
  }, [field.name]);

  if (!field) return null;

  const hasChildren = field.children && field.children.length > 0;
  const color = getFieldColor(field);

  const toggleExpand = (e) => {
    e.stopPropagation();
    if (hasChildren) {
      setIsExpanded(!isExpanded);
    }
  };

  const copyFieldPath = (e) => {
    e.stopPropagation();
    navigator.clipboard.writeText(fullPath);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const getTypeIcon = () => {
    switch (field.kind) {
      case 'struct':
        return <Box size={12} />;
      case 'array':
        return <List size={12} />;
      case 'map':
        return <Hash size={12} />;
      case 'primitive':
        switch (field.type) {
          case 'string':
            return <Type size={12} />;
          case 'boolean':
            return <ToggleLeft size={12} />;
          case 'timestamp':
          case 'date':
            return <Calendar size={12} />;
          default:
            return <Hash size={12} />;
        }
      default:
        return <Circle size={12} />;
    }
  };

  const displayType = formatFieldForDisplay(field);

  return (
    <div className={`field-tree-item ${isLast ? 'last' : ''}`} style={{ '--level': level }}>
      <div
        className={`field-row ${hasChildren ? 'expandable' : ''} ${isExpanded ? 'expanded' : ''}`}
        onClick={toggleExpand}
        onMouseEnter={() => isTruncated && setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
      >
        {/* Tree Line Indicator */}
        {level > 0 && <div className="tree-line" />}

        {/* Expand/Collapse Icon */}
        <div className="expand-icon">
          {hasChildren ? (
            isExpanded ? (
              <ChevronDown size={14} className="chevron" />
            ) : (
              <ChevronRight size={14} className="chevron" />
            )
          ) : (
            <span className="dot" style={{ background: color }} />
          )}
        </div>

        {/* Field Content - Name and Type */}
        <div className="field-content">
          <div className="field-name-wrapper">
            <span
              ref={nameRef}
              className="field-name-text"
              title={field.name}
            >
              {field.name}
            </span>
            {!nullable && <span className="required-indicator" title="Required field">*</span>}

            {/* Copy button - appears on hover */}
            <button
              className="copy-path-btn"
              onClick={copyFieldPath}
              title={`Copy path: ${fullPath}`}
            >
              {copied ? <Check size={10} /> : <Copy size={10} />}
            </button>
          </div>

          {/* Type Badge */}
          <div
            className={`type-badge type-${field.kind}`}
            style={{ '--type-color': color }}
          >
            <span className="type-icon">{getTypeIcon()}</span>
            <span className="type-text">{displayType}</span>
            {hasChildren && !isExpanded && (
              <span className="child-count">
                {field.kind === 'struct' && `${field.fieldCount}`}
                {field.kind === 'array' && '[]'}
                {field.kind === 'map' && '{}'}
              </span>
            )}
          </div>
        </div>

        {/* Tooltip for truncated names */}
        {showTooltip && (
          <div className="field-tooltip">
            <div className="tooltip-name">{field.name}</div>
            <div className="tooltip-path">{fullPath}</div>
          </div>
        )}
      </div>

      {/* Nested Children */}
      {hasChildren && isExpanded && (
        <div className="field-children">
          {field.children.map((child, idx) => (
            <FieldTree
              key={`${child.name}-${idx}`}
              field={child}
              level={level + 1}
              nullable={child.nullable !== false}
              isLast={idx === field.children.length - 1}
              parentPath={fullPath}
            />
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * SchemaTree Component
 *
 * Renders a complete schema with all fields
 * Parses raw Spark types and displays them hierarchically
 *
 * @param {array} fields - Array of field objects with { name, type, nullable }
 * @param {number} maxVisible - Maximum fields to show before "Show more" (default: all)
 * @param {boolean} compact - Use compact mode for smaller containers
 */
export function SchemaTree({ fields, maxVisible = null, compact = false }) {
  const [showAll, setShowAll] = useState(maxVisible === null);
  const [expandAll, setExpandAll] = useState(false);

  if (!fields || fields.length === 0) {
    return (
      <div className="schema-tree-empty">
        <Circle size={16} />
        <span>No fields defined</span>
      </div>
    );
  }

  // Parse all fields
  const parsedFields = fields.map((f) => ({
    name: f.name,
    nullable: f.nullable !== false,
    ...parseSparkType(f.type || 'unknown'),
  }));

  const visibleFields = showAll || !maxVisible
    ? parsedFields
    : parsedFields.slice(0, maxVisible);

  const hiddenCount = parsedFields.length - visibleFields.length;

  // Count complex types for info display
  const complexCount = parsedFields.filter(f => f.kind !== 'primitive').length;

  return (
    <div className={`schema-tree ${compact ? 'compact' : ''}`}>
      {/* Schema Summary Header */}
      <div className="schema-tree-header">
        <span className="field-count">
          {parsedFields.length} field{parsedFields.length !== 1 ? 's' : ''}
          {complexCount > 0 && (
            <span className="complex-count"> ({complexCount} nested)</span>
          )}
        </span>
      </div>

      {/* Fields List */}
      <div className="schema-tree-fields">
        {visibleFields.map((field, idx) => (
          <FieldTree
            key={`${field.name}-${idx}`}
            field={field}
            level={0}
            nullable={field.nullable}
            isLast={showAll ? idx === parsedFields.length - 1 : idx === visibleFields.length - 1}
            parentPath=""
          />
        ))}
      </div>

      {/* Show More/Less Controls */}
      <div className="schema-tree-controls">
        {!showAll && hiddenCount > 0 && (
          <button
            className="schema-tree-btn show-more"
            onClick={() => setShowAll(true)}
          >
            <ChevronDown size={12} />
            <span>Show {hiddenCount} more field{hiddenCount !== 1 ? 's' : ''}</span>
          </button>
        )}

        {showAll && maxVisible && parsedFields.length > maxVisible && (
          <button
            className="schema-tree-btn show-less"
            onClick={() => setShowAll(false)}
          >
            <ChevronRight size={12} />
            <span>Collapse</span>
          </button>
        )}
      </div>
    </div>
  );
}

export default FieldTree;
