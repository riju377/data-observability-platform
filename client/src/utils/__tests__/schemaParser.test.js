import { parseSparkType, formatFieldForDisplay } from '../schemaParser';

/**
 * Test cases for schema parser
 * Run with: npm test
 */

describe('Schema Parser', () => {
  test('parses primitive types', () => {
    expect(parseSparkType('string')).toMatchObject({ kind: 'primitive', type: 'string' });
    expect(parseSparkType('int')).toMatchObject({ kind: 'primitive', type: 'int' });
    expect(parseSparkType('boolean')).toMatchObject({ kind: 'primitive', type: 'boolean' });
  });

  test('parses simple struct', () => {
    const result = parseSparkType('struct<name:string,age:int>');
    expect(result.kind).toBe('struct');
    expect(result.children).toHaveLength(2);
    expect(result.children[0]).toMatchObject({ name: 'name', kind: 'primitive', type: 'string' });
    expect(result.children[1]).toMatchObject({ name: 'age', kind: 'primitive', type: 'int' });
  });

  test('parses deeply nested struct (like device)', () => {
    const deviceType = 'struct<device_name:string,device_model:string,hwv:string,device_vendor:string,device_category:string,manufacturer:string,year_released:string,platform:string,major_os:string,md5_dpid:string,sha1_dpid:string,md5_did:string,sha1_did:string,ifa:string,user_agent:string>';

    const result = parseSparkType(deviceType);

    expect(result.kind).toBe('struct');
    expect(result.children).toHaveLength(15);
    expect(result.children[0]).toMatchObject({ name: 'device_name', kind: 'primitive', type: 'string' });
    expect(result.children[14]).toMatchObject({ name: 'user_agent', kind: 'primitive', type: 'string' });
  });

  test('parses struct with nested struct', () => {
    const complexType = 'struct<user:struct<yob:int,gender:string>,device:struct<name:string,model:string>>';

    const result = parseSparkType(complexType);

    expect(result.kind).toBe('struct');
    expect(result.children).toHaveLength(2);

    // Check user field
    const userField = result.children[0];
    expect(userField.name).toBe('user');
    expect(userField.kind).toBe('struct');
    expect(userField.children).toHaveLength(2);

    // Check device field
    const deviceField = result.children[1];
    expect(deviceField.name).toBe('device');
    expect(deviceField.kind).toBe('struct');
    expect(deviceField.children).toHaveLength(2);
  });

  test('parses array types', () => {
    const arrayType = 'array<string>';
    const result = parseSparkType(arrayType);

    expect(result.kind).toBe('array');
    expect(result.elementType).toBe('string');
  });

  test('parses array of structs', () => {
    const arrayStructType = 'array<struct<id:int,name:string>>';
    const result = parseSparkType(arrayStructType);

    expect(result.kind).toBe('array');
    expect(result.children[0].kind).toBe('struct');
    expect(result.children[0].children).toHaveLength(2);
  });

  test('parses map types', () => {
    const mapType = 'map<string,int>';
    const result = parseSparkType(mapType);

    expect(result.kind).toBe('map');
    expect(result.keyType).toBe('string');
    expect(result.valueType).toBe('int');
  });

  test('parses map with struct values', () => {
    const complexMapType = 'map<string,struct<count:int,sum:double>>';
    const result = parseSparkType(complexMapType);

    expect(result.kind).toBe('map');
    expect(result.children[1].kind).toBe('struct'); // value child
    expect(result.children[1].children).toHaveLength(2);
  });

  test('formats display correctly', () => {
    const structField = { kind: 'struct', fieldCount: 5 };
    const arrayField = { kind: 'array', elementType: 'string' };
    const mapField = { kind: 'map', keyType: 'string', valueType: 'int' };

    expect(formatFieldForDisplay(structField)).toBe('struct (5 fields)');
    expect(formatFieldForDisplay(arrayField)).toBe('array<string>');
    expect(formatFieldForDisplay(mapField)).toBe('map<string,int>');
  });
});

// Example usage for documentation
console.log('Example: Complex nested schema parsing');
console.log('======================================');

const exampleSchema = {
  name: 'user_events',
  fields: [
    {
      name: 'event_id',
      type: 'string',
      nullable: false
    },
    {
      name: 'timestamp',
      type: 'timestamp',
      nullable: false
    },
    {
      name: 'user',
      type: 'struct<yob:int,gender:string>',
      nullable: true
    },
    {
      name: 'device',
      type: 'struct<device_name:string,device_model:string,hwv:string,device_vendor:string,device_category:string,manufacturer:string,year_released:string,platform:string,major_os:string,md5_dpid:string,sha1_dpid:string,md5_did:string,sha1_did:string,ifa:string,user_agent:string>',
      nullable: true
    },
    {
      name: 'tags',
      type: 'array<string>',
      nullable: true
    },
    {
      name: 'metadata',
      type: 'map<string,string>',
      nullable: true
    }
  ]
};

console.log('Input schema:', JSON.stringify(exampleSchema, null, 2));

console.log('\nParsed device field:');
const deviceParsed = parseSparkType(exampleSchema.fields[3].type);
console.log(JSON.stringify(deviceParsed, null, 2));