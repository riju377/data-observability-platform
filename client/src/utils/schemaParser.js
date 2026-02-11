/**
 * Schema Parser Utility
 *
 * Parses Spark schema types (struct, array, map) into hierarchical tree structure
 * Handles complex nested types like:
 * - struct<field1:type1,field2:type2>
 * - array<struct<...>>
 * - map<key_type,value_type>
 */

/**
 * Parse a Spark type string into a structured object
 *
 * @param {string} typeStr - Spark type string (e.g., "struct<name:string,age:int>")
 * @returns {object} - Parsed type object with { kind, children, ... }
 */
export function parseSparkType(typeStr) {
  if (!typeStr || typeof typeStr !== 'string') {
    return { kind: 'primitive', type: typeStr || 'unknown', children: [] };
  }

  const trimmed = typeStr.trim();

  // Primitive types (int, string, boolean, etc.)
  if (!trimmed.includes('<') && !trimmed.includes('(')) {
    return { kind: 'primitive', type: trimmed, children: [] };
  }

  // Struct type: struct<field1:type1,field2:type2,...>
  if (trimmed.startsWith('struct<')) {
    const innerContent = extractBetweenBrackets(trimmed, 'struct');
    const fields = parseStructFields(innerContent);
    return {
      kind: 'struct',
      type: 'struct',
      children: fields,
      fieldCount: fields.length
    };
  }

  // Array type: array<element_type>
  if (trimmed.startsWith('array<')) {
    const innerContent = extractBetweenBrackets(trimmed, 'array');
    const elementType = parseSparkType(innerContent);
    return {
      kind: 'array',
      type: 'array',
      children: [{ name: 'element', ...elementType }],
      elementType: innerContent
    };
  }

  // Map type: map<key_type,value_type>
  if (trimmed.startsWith('map<')) {
    const innerContent = extractBetweenBrackets(trimmed, 'map');
    const [keyType, valueType] = parseMapTypes(innerContent);
    return {
      kind: 'map',
      type: 'map',
      children: [
        { name: 'key', ...parseSparkType(keyType) },
        { name: 'value', ...parseSparkType(valueType) }
      ],
      keyType,
      valueType
    };
  }

  // Fallback for unknown complex types
  return { kind: 'primitive', type: trimmed, children: [] };
}

/**
 * Extract content between angle brackets for a given type
 * Example: "struct<a:int,b:string>" -> "a:int,b:string"
 */
function extractBetweenBrackets(typeStr, prefix) {
  const startIdx = typeStr.indexOf('<');
  const endIdx = typeStr.lastIndexOf('>');

  if (startIdx === -1 || endIdx === -1) {
    return '';
  }

  return typeStr.substring(startIdx + 1, endIdx);
}

/**
 * Parse struct fields: "field1:type1,field2:type2,field3:struct<...>,..."
 * Handles nested structs by respecting bracket depth
 */
function parseStructFields(content) {
  if (!content || content.trim() === '') {
    return [];
  }

  const fields = [];
  let currentField = '';
  let bracketDepth = 0;
  let inBackticks = false;

  for (let i = 0; i < content.length; i++) {
    const char = content[i];

    // Handle backtick-quoted field names (e.g., `field-name-with-dashes`)
    if (char === '`') {
      inBackticks = !inBackticks;
      currentField += char;
      continue;
    }

    // Track bracket depth for nested types
    if (!inBackticks) {
      if (char === '<') bracketDepth++;
      if (char === '>') bracketDepth--;

      // Split on comma only at top level (bracketDepth === 0)
      if (char === ',' && bracketDepth === 0) {
        if (currentField.trim()) {
          fields.push(parseStructField(currentField.trim()));
        }
        currentField = '';
        continue;
      }
    }

    currentField += char;
  }

  // Don't forget the last field
  if (currentField.trim()) {
    fields.push(parseStructField(currentField.trim()));
  }

  return fields;
}

/**
 * Parse a single struct field: "field_name:field_type"
 */
function parseStructField(fieldStr) {
  // Handle backtick-quoted names: `field-name`:type
  const colonIdx = fieldStr.indexOf(':');

  if (colonIdx === -1) {
    return {
      name: fieldStr,
      ...parseSparkType('unknown'),
      raw: fieldStr
    };
  }

  let fieldName = fieldStr.substring(0, colonIdx).trim();
  const fieldType = fieldStr.substring(colonIdx + 1).trim();

  // Remove backticks from field name if present
  if (fieldName.startsWith('`') && fieldName.endsWith('`')) {
    fieldName = fieldName.substring(1, fieldName.length - 1);
  }

  return {
    name: fieldName,
    ...parseSparkType(fieldType),
    raw: fieldType
  };
}

/**
 * Parse map types: "key_type,value_type"
 * Handles nested types in values
 */
function parseMapTypes(content) {
  let bracketDepth = 0;
  let splitIdx = -1;

  for (let i = 0; i < content.length; i++) {
    const char = content[i];

    if (char === '<') bracketDepth++;
    if (char === '>') bracketDepth--;

    if (char === ',' && bracketDepth === 0) {
      splitIdx = i;
      break;
    }
  }

  if (splitIdx === -1) {
    return [content.trim(), 'unknown'];
  }

  const keyType = content.substring(0, splitIdx).trim();
  const valueType = content.substring(splitIdx + 1).trim();

  return [keyType, valueType];
}

/**
 * Format a schema field for display
 * Recursively handles nested structures
 */
export function formatFieldForDisplay(field) {
  if (!field) return 'unknown';

  switch (field.kind) {
    case 'primitive':
      return field.type;

    case 'struct':
      return `struct (${field.fieldCount || 0} fields)`;

    case 'array':
      if (field.elementType) {
        const parsed = parseSparkType(field.elementType);
        if (parsed.kind === 'primitive') {
          return `array<${parsed.type}>`;
        }
        return `array<${formatFieldForDisplay(parsed)}>`;
      }
      return 'array';

    case 'map':
      return `map<${field.keyType || '?'},${field.valueType || '?'}>`;

    default:
      return field.type || 'unknown';
  }
}

/**
 * Get icon name for field type
 */
export function getFieldIcon(field) {
  if (!field) return 'circle';

  switch (field.kind) {
    case 'struct':
      return 'box';
    case 'array':
      return 'list';
    case 'map':
      return 'hash';
    case 'primitive':
      switch (field.type) {
        case 'string':
          return 'type';
        case 'int':
        case 'long':
        case 'double':
        case 'float':
        case 'decimal':
          return 'hash';
        case 'boolean':
          return 'toggle-left';
        case 'timestamp':
        case 'date':
          return 'calendar';
        default:
          return 'circle';
      }
    default:
      return 'circle';
  }
}

/**
 * Get color for field type
 */
export function getFieldColor(field) {
  if (!field) return '#888';

  switch (field.kind) {
    case 'struct':
      return '#9c27b0'; // Purple
    case 'array':
      return '#2196f3'; // Blue
    case 'map':
      return '#ff9800'; // Orange
    case 'primitive':
      switch (field.type) {
        case 'string':
          return '#4caf50'; // Green
        case 'int':
        case 'long':
        case 'double':
        case 'float':
        case 'decimal':
          return '#f44336'; // Red
        case 'boolean':
          return '#00bcd4'; // Cyan
        case 'timestamp':
        case 'date':
          return '#673ab7'; // Deep Purple
        default:
          return '#607d8b'; // Blue Grey
      }
    default:
      return '#888';
  }
}