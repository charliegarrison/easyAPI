import { Pool, PoolClient } from 'pg';

/**
 * JSON Schema to PostgreSQL JSONB Table Generator
 * 
 * Creates/updates PostgreSQL tables that store data as JSONB with:
 * - Automatic table creation
 * - GIN indexes for JSONB queries
 * - Generated columns for indexed fields
 * - Full-text search support
 * - Automatic updated_at triggers
 * 
 * NOTE: Validation is NOT done at the database level.
 * Use separate application-layer validation functions.
 */

// ============================================================================
// Types
// ============================================================================

export interface JSONSchemaProperty {
  type: string | string[];
  description?: string;
  enum?: any[];
  format?: string;
  pattern?: string;
  minimum?: number;
  maximum?: number;
  minLength?: number;
  maxLength?: number;
  items?: JSONSchemaProperty;
  properties?: Record<string, JSONSchemaProperty>;
  required?: string[];
  $ref?: string;
  default?: any;
}

export interface JSONSchema {
  $id?: string;
  $schema?: string;
  title?: string;
  description?: string;
  type: string;
  properties: Record<string, JSONSchemaProperty>;
  required?: string[];
  additionalProperties?: boolean;
}

export interface TableConfig {
  tableName: string;
  // Schema is OPTIONAL - used for documentation/reference only (validation done in app layer)
  schema?: JSONSchema;
  // Additional columns beyond id, data, created_at, updated_at
  additionalColumns?: AdditionalColumn[];
  // Fields to extract from JSONB into indexed generated columns
  indexedFields?: IndexedField[];
  // Custom indexes on JSONB data (JSON-based definition)
  indexes?: JsonIndex[];
  // Whether to add full-text search
  enableFullTextSearch?: boolean;
  fullTextSearchFields?: string[];
}

export interface AdditionalColumn {
  name: string;
  type: string;           // Base type: 'UUID', 'TEXT', 'TIMESTAMPTZ', etc.
  nullable?: boolean;
  default?: string;
  unique?: boolean;
  primaryKey?: boolean;   // Set as primary key
}

export interface IndexedField {
  jsonPath: string;        // e.g., 'email' or 'address.city'
  columnName: string;      // Generated column name
  pgType: string;          // PostgreSQL type: 'TEXT', 'INTEGER', 'BOOLEAN', 'NUMERIC', etc.
  unique?: boolean;        // Create unique index on generated column
  nullable?: boolean;      // Allow nulls
  index?: boolean;         // Whether to create an index (default: true)
}

/**
 * PostgreSQL index types
 */
export enum IndexType {
  BTREE = 'btree',
  GIN = 'gin',
  HASH = 'hash',
  EXPRESSION = 'expression'
}

/**
 * JSON-based index definition for JSONB fields
 * Allows creating various index types without writing SQL
 */
export interface JsonIndex {
  name?: string;           // Optional custom index name (auto-generated if not provided)
  type: IndexType;         // Index type (use IndexType enum)
  fields?: string[];       // JSON paths to index (e.g., ['clientId', 'address.city'])
  columns?: string[];      // Actual column names to index (e.g., ['client_id', 'status'])
  expression?: string;     // For expression index: custom SQL expression
  unique?: boolean;        // Create unique index
  where?: string;          // Partial index: JSON path condition (e.g., 'status = active')
}

export interface GeneratedScripts {
  createTable: string;
  createIndexes: string;
  createTriggers: string;
  migration: string;       // Combined script for fresh install
  updateScript: string;    // Script that handles existing tables
}

// ============================================================================
// Main Generator Class
// ============================================================================

export class JsonSchemaToPostgres {
  private pool: Pool | null = null;

  constructor(pool?: Pool) {
    this.pool = pool || null;
  }

  /**
   * Generate all SQL scripts for a table configuration
   */
  generateScripts(config: TableConfig): GeneratedScripts {
    const { tableName, schema, additionalColumns, indexedFields, indexes, enableFullTextSearch, fullTextSearchFields } = config;

    const createTable = this.generateCreateTable(tableName, additionalColumns, indexedFields, enableFullTextSearch, fullTextSearchFields);
    const createIndexes = this.generateIndexes(tableName, indexedFields, indexes, enableFullTextSearch);
    const createTriggers = this.generateTriggers(tableName);
    
    const migration = `-- Migration script for table: ${tableName}
-- Schema reference: ${schema?.title || 'Schema-less'}
-- Generated at: ${new Date().toISOString()}
-- NOTE: Validation is handled in application layer, not in PostgreSQL
-- NOTE: Only built-in column is 'data' (JSONB). Add id, timestamps, etc. via additionalColumns.

-- ============================================================================
-- Table Creation
-- ============================================================================

${createTable}

-- ============================================================================
-- Indexes
-- ============================================================================

${createIndexes}
`;

    const updateScript = this.generateUpdateScript(tableName, additionalColumns, indexedFields, indexes, enableFullTextSearch, fullTextSearchFields);

    return {
      createTable,
      createIndexes,
      createTriggers,
      migration,
      updateScript
    };
  }

  /**
   * Generate CREATE TABLE statement
   */
  private generateCreateTable(
    tableName: string, 
    additionalColumns?: AdditionalColumn[],
    indexedFields?: IndexedField[],
    enableFullTextSearch?: boolean,
    fullTextSearchFields?: string[]
  ): string {
    const columns: string[] = [
      `data JSONB NOT NULL`  // Only built-in column - everything else via additionalColumns
    ];

    // Add additional columns
    if (additionalColumns) {
      for (const col of additionalColumns) {
        let colDef = `${col.name} ${col.type}`;
        if (col.primaryKey) colDef += ' PRIMARY KEY';
        if (!col.nullable && !col.primaryKey) colDef += ' NOT NULL';
        if (col.default) colDef += ` DEFAULT ${col.default}`;
        if (col.unique && !col.primaryKey) colDef += ' UNIQUE';
        columns.push(colDef);
      }
    }

    // Add generated columns for indexed fields
    if (indexedFields) {
      for (const field of indexedFields) {
        const jsonPathExpr = this.buildJsonPathExpression(field.jsonPath);
        let colDef = `${field.columnName} ${field.pgType} GENERATED ALWAYS AS ((data${jsonPathExpr})::${field.pgType}) STORED`;
        columns.push(colDef);
      }
    }

    // Add full-text search vector column
    if (enableFullTextSearch && fullTextSearchFields && fullTextSearchFields.length > 0) {
      const tsvectorExpr = fullTextSearchFields
        .map(f => `COALESCE(data->>'${f}', '')`)
        .join(" || ' ' || ");
      columns.push(`search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', ${tsvectorExpr})) STORED`);
    }

    return `CREATE TABLE IF NOT EXISTS ${tableName} (
  ${columns.join(',\n  ')}
);`;
  }

  /**
   * Build JSON path expression for PostgreSQL
   */
  private buildJsonPathExpression(jsonPath: string): string {
    const parts = jsonPath.split('.');
    if (parts.length === 1) {
      return `->>'${parts[0]}'`;
    }
    // For nested paths: data->'address'->>'city'
    const lastPart = parts.pop()!;
    const middleParts = parts.map(p => `->'${p}'`).join('');
    return `${middleParts}->>'${lastPart}'`;
  }

  /**
   * Generate index creation statements
   */
  private generateIndexes(
    tableName: string,
    indexedFields?: IndexedField[],
    customIndexes?: JsonIndex[],
    enableFullTextSearch?: boolean
  ): string {
    const indexStatements: string[] = [
      `-- GIN index for JSONB queries`,
      `CREATE INDEX IF NOT EXISTS idx_${tableName}_data_gin ON ${tableName} USING GIN (data);`
    ];

    // Indexes for generated columns (from indexedFields)
    if (indexedFields) {
      indexStatements.push('', '-- Indexes for generated columns');
      for (const field of indexedFields) {
        if (field.index === false) continue; // Skip if explicitly disabled
        const indexType = field.unique ? 'UNIQUE INDEX' : 'INDEX';
        indexStatements.push(
          `CREATE ${indexType} IF NOT EXISTS idx_${tableName}_${field.columnName} ON ${tableName} (${field.columnName});`
        );
      }
    }

    // Custom JSON-defined indexes
    if (customIndexes && customIndexes.length > 0) {
      indexStatements.push('', '-- Custom JSONB indexes');
      for (const idx of customIndexes) {
        const indexSql = this.buildCustomIndex(tableName, idx);
        if (indexSql) {
          indexStatements.push(indexSql);
        }
      }
    }

    // Full-text search index
    if (enableFullTextSearch) {
      indexStatements.push(
        '',
        '-- Full-text search index',
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_search ON ${tableName} USING GIN (search_vector);`
      );
    }

    return indexStatements.join('\n');
  }

  /**
   * Build a custom index SQL statement from JSON definition
   */
  private buildCustomIndex(tableName: string, idx: JsonIndex): string {
    const unique = idx.unique ? 'UNIQUE ' : '';
    const hasFields = idx.fields && idx.fields.length > 0;
    const hasColumns = idx.columns && idx.columns.length > 0;
    const indexName = idx.name || `idx_${tableName}_${idx.type}_${idx.columns?.join('_') || idx.fields?.join('_') || 'custom'}`;
    
    let indexDef = '';
    
    switch (idx.type) {
      case IndexType.BTREE:
        // B-tree index on columns or JSONB fields
        if (!hasFields && !hasColumns) return '';
        
        // Build expression list - columns are used directly, fields are JSON paths
        const btreeExprs: string[] = [];
        if (hasColumns) {
          btreeExprs.push(...idx.columns!);
        }
        if (hasFields) {
          btreeExprs.push(...idx.fields!.map(f => `(data->>'${f}')`));
        }
        indexDef = `CREATE ${unique}INDEX IF NOT EXISTS ${indexName} ON ${tableName} (${btreeExprs.join(', ')})`;
        break;
        
      case IndexType.GIN:
        // GIN index - can be on specific path, column, or expression
        if (hasColumns) {
          // GIN on a JSONB column
          indexDef = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName} USING GIN (${idx.columns![0]})`;
        } else if (hasFields) {
          // GIN on specific JSON paths using jsonb_path_ops for containment queries
          const ginExpr = idx.fields!.length === 1 
            ? `(data->'${idx.fields![0]}')` 
            : `data`;
          indexDef = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName} USING GIN (${ginExpr} jsonb_path_ops)`;
        } else if (idx.expression) {
          indexDef = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName} USING GIN (${idx.expression})`;
        } else {
          return ''; // Need fields, columns, or expression
        }
        break;
        
      case IndexType.HASH:
        // Hash index - good for equality comparisons only, single column/field
        if (hasColumns) {
          indexDef = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName} USING HASH (${idx.columns![0]})`;
        } else if (hasFields && idx.fields!.length === 1) {
          indexDef = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName} USING HASH ((data->>'${idx.fields![0]}'))`;
        } else {
          return ''; // Hash only supports single column/field
        }
        break;
        
      case IndexType.EXPRESSION:
        // Custom expression index
        if (!idx.expression) return '';
        indexDef = `CREATE ${unique}INDEX IF NOT EXISTS ${indexName} ON ${tableName} (${idx.expression})`;
        break;
        
      default:
        return '';
    }
    
    // Add partial index WHERE clause if specified
    if (idx.where) {
      // Parse simple JSON path conditions like "status = active" to SQL
      const whereClause = this.parseWhereCondition(idx.where);
      indexDef += ` WHERE ${whereClause}`;
    }
    
    return indexDef + ';';
  }

  /**
   * Parse a simple where condition from JSON-friendly format to SQL
   * Supports: "field = value", "field != value", "field IS NOT NULL"
   */
  private parseWhereCondition(condition: string): string {
    // Handle IS NOT NULL
    if (condition.toLowerCase().includes('is not null')) {
      const field = condition.split(/\s+is\s+not\s+null/i)[0].trim();
      return `data->>'${field}' IS NOT NULL`;
    }
    
    // Handle IS NULL
    if (condition.toLowerCase().includes('is null')) {
      const field = condition.split(/\s+is\s+null/i)[0].trim();
      return `data->>'${field}' IS NULL`;
    }
    
    // Handle != or <>
    if (condition.includes('!=') || condition.includes('<>')) {
      const [field, value] = condition.split(/!=|<>/).map(s => s.trim());
      return `data->>'${field}' != '${value}'`;
    }
    
    // Handle =
    if (condition.includes('=')) {
      const [field, value] = condition.split('=').map(s => s.trim());
      return `data->>'${field}' = '${value}'`;
    }
    
    // Return as-is if can't parse (user provided raw SQL)
    return condition;
  }

  /**
   * Generate trigger (none by default - add custom triggers via additionalColumns if needed)
   */
  private generateTriggers(tableName: string): string {
    // No built-in triggers - table only has data column
    // If you need updated_at, add it via additionalColumns and create your own trigger
    return `-- No built-in triggers (add custom triggers as needed)`;
  }

  /**
   * Generate update script that handles existing tables
   */
  private generateUpdateScript(
    tableName: string,
    additionalColumns?: AdditionalColumn[],
    indexedFields?: IndexedField[],
    customIndexes?: JsonIndex[],
    enableFullTextSearch?: boolean,
    fullTextSearchFields?: string[]
  ): string {
    return `-- Update script for table: ${tableName}
-- This script can be run multiple times safely (idempotent)
-- Generated at: ${new Date().toISOString()}
-- NOTE: Validation is handled in application layer, not in PostgreSQL

DO $$
BEGIN
  -- Check if table exists
  IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = '${tableName}') THEN
    -- Table doesn't exist, create it
    RAISE NOTICE 'Creating table ${tableName}...';
    
    ${this.generateCreateTable(tableName, additionalColumns, indexedFields, enableFullTextSearch, fullTextSearchFields).replace(/\n/g, '\n    ')}
  ELSE
    RAISE NOTICE 'Table ${tableName} already exists, checking for updates...';
    
    ${this.generateColumnUpdates(tableName, additionalColumns, indexedFields, enableFullTextSearch, fullTextSearchFields)}
  END IF;
END $$;

-- Ensure indexes exist
${this.generateIndexes(tableName, indexedFields, customIndexes, enableFullTextSearch)}
`;
  }

  /**
   * Generate column update statements for existing tables
   */
  private generateColumnUpdates(
    tableName: string,
    additionalColumns?: AdditionalColumn[],
    indexedFields?: IndexedField[],
    enableFullTextSearch?: boolean,
    fullTextSearchFields?: string[]
  ): string {
    const updates: string[] = [];

    // Check and add additional columns
    if (additionalColumns) {
      for (const col of additionalColumns) {
        // Build column definition for ALTER TABLE
        let colDef = `${col.name} ${col.type}`;
        if (col.primaryKey) colDef += ' PRIMARY KEY';
        if (!col.nullable && !col.primaryKey) colDef += ' NOT NULL';
        if (col.default) colDef += ` DEFAULT ${col.default}`;
        if (col.unique && !col.primaryKey) colDef += ' UNIQUE';
        
        updates.push(`
    -- Add column ${col.name} if not exists
    IF NOT EXISTS (SELECT FROM information_schema.columns 
                   WHERE table_name = '${tableName}' AND column_name = '${col.name}') THEN
      ALTER TABLE ${tableName} ADD COLUMN ${colDef};
      RAISE NOTICE 'Added column ${col.name}';
    END IF;`);
      }
    }

    // Check and add indexed generated columns
    if (indexedFields) {
      for (const field of indexedFields) {
        const jsonPathExpr = this.buildJsonPathExpression(field.jsonPath);
        updates.push(`
    -- Add generated column ${field.columnName} if not exists
    IF NOT EXISTS (SELECT FROM information_schema.columns 
                   WHERE table_name = '${tableName}' AND column_name = '${field.columnName}') THEN
      ALTER TABLE ${tableName} ADD COLUMN ${field.columnName} ${field.pgType} 
        GENERATED ALWAYS AS ((data${jsonPathExpr})::${field.pgType}) STORED;
      RAISE NOTICE 'Added generated column ${field.columnName}';
    END IF;`);
      }
    }

    // Check and add search vector column
    if (enableFullTextSearch && fullTextSearchFields && fullTextSearchFields.length > 0) {
      const tsvectorExpr = fullTextSearchFields
        .map(f => `COALESCE(data->>'${f}', '')`)
        .join(" || ' ' || ");
      updates.push(`
    -- Add search_vector column if not exists
    IF NOT EXISTS (SELECT FROM information_schema.columns 
                   WHERE table_name = '${tableName}' AND column_name = 'search_vector') THEN
      ALTER TABLE ${tableName} ADD COLUMN search_vector TSVECTOR 
        GENERATED ALWAYS AS (to_tsvector('english', ${tsvectorExpr})) STORED;
      RAISE NOTICE 'Added search_vector column';
    END IF;`);
    }

    return updates.join('\n');
  }

  // ============================================================================
  // Database Execution Methods
  // ============================================================================

  /**
   * Execute the migration script against the database
   */
  async migrate(config: TableConfig, debug: boolean = false): Promise<void> {
    if (!this.pool) {
      throw new Error('No database pool configured. Pass a Pool instance to the constructor.');
    }

    const scripts = this.generateScripts(config);
    
    if (debug) {
      console.log('\n========== MIGRATION SQL DEBUG ==========');
      console.log(`Table: ${config.tableName}`);
      console.log('\n--- Update Script ---');
      console.log(scripts.updateScript);
      console.log('\n--- Create Table ---');
      console.log(scripts.createTable);
      console.log('\n--- Create Indexes ---');
      console.log(scripts.createIndexes);
      console.log('==========================================\n');
    }

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');
      await client.query(scripts.updateScript);
      await client.query('COMMIT');
      console.log(`Successfully migrated table: ${config.tableName}`);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Migrate multiple tables
   */
  async migrateAll(configs: TableConfig[]): Promise<void> {
    for (const config of configs) {
      await this.migrate(config);
    }
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Quick function to generate scripts without instantiating the class
 */
export function generatePostgresScripts(config: TableConfig): GeneratedScripts {
  const generator = new JsonSchemaToPostgres();
  return generator.generateScripts(config);
}

// ============================================================================
// Example Usage
// ============================================================================

/*
// Example JSON Schema
const complaintSchema: JSONSchema = {
  $id: 'complaint',
  title: 'Complaint',
  description: 'A customer complaint record',
  type: 'object',
  properties: {
    complainantName: {
      type: 'string',
      minLength: 1,
      maxLength: 255
    },
    complainantEmail: {
      type: 'string',
      format: 'email'
    },
    complainantPhone: {
      type: 'string',
      pattern: '^[0-9-+()\\s]+$'
    },
    establishmentName: {
      type: 'string',
      minLength: 1
    },
    establishmentAddress: {
      type: 'string'
    },
    complaintType: {
      type: 'string',
      enum: ['food_safety', 'sanitation', 'pest', 'employee', 'other']
    },
    description: {
      type: 'string',
      minLength: 10
    },
    severity: {
      type: 'integer',
      minimum: 1,
      maximum: 5
    },
    status: {
      type: 'string',
      enum: ['new', 'in_progress', 'resolved', 'closed'],
      default: 'new'
    }
  },
  required: ['complainantName', 'establishmentName', 'complaintType', 'description']
};

// Table configuration
const config: TableConfig = {
  tableName: 'complaints',
  schema: complaintSchema,
  
  // Add id and timestamps as additional columns (not built-in anymore)
  additionalColumns: [
    { name: 'id', type: 'UUID', primaryKey: true },
    { name: 'created_at', type: 'TIMESTAMPTZ', nullable: false, default: 'NOW()' },
    { name: 'updated_at', type: 'TIMESTAMPTZ', nullable: false, default: 'NOW()' }
  ],
  
  // Generated columns - extract JSONB fields into real columns for best query performance
  indexedFields: [
    { jsonPath: 'complainantEmail', columnName: 'email', pgType: 'TEXT' },
    { jsonPath: 'status', columnName: 'status', pgType: 'TEXT' },
    { jsonPath: 'clientId', columnName: 'client_id', pgType: 'TEXT' }
  ],
  
  // Custom indexes - JSON-based definition (no SQL required)
  indexes: [
    // Index on a generated COLUMN (from indexedFields above)
    // Use 'columns' for real table columns
    { type: IndexType.BTREE, columns: ['client_id'] },
    
    // Index directly on JSONB field (no generated column needed)
    // Use 'fields' for JSON paths - becomes (data->>'fieldName')
    { type: IndexType.BTREE, fields: ['clientId'] },
    
    // Composite index mixing columns and JSONB fields
    { type: IndexType.BTREE, columns: ['status'], fields: ['priority'], name: 'idx_status_priority' },
    
    // Unique index on a JSONB field
    { type: IndexType.BTREE, fields: ['referenceNumber'], unique: true },
    
    // Hash index on a column - very fast for equality only
    { type: IndexType.HASH, columns: ['email'] },
    
    // Partial index - only index active complaints (saves space)
    { type: IndexType.BTREE, columns: ['status'], where: 'status != closed' },
    
    // Expression index for case-insensitive search
    { type: IndexType.EXPRESSION, expression: "LOWER(data->>'complainantEmail')" },
    
    // GIN index on a nested JSONB object for containment queries
    { type: IndexType.GIN, fields: ['metadata'] }
  ],
  
  enableFullTextSearch: true,
  fullTextSearchFields: ['complainantName', 'establishmentName', 'description']
};

// Generate scripts
const scripts = generatePostgresScripts(config);
console.log(scripts.migration);

// Or with database connection
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const generator = new JsonSchemaToPostgres(pool);
await generator.migrate(config);
*/

export default JsonSchemaToPostgres;
