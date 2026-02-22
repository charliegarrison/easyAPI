import { Pool } from 'pg';
import express, { Request, Response, Router } from 'express';
import { validate } from 'jsonschema';
import { v7 as uuidv7 } from 'uuid';
import crypto from 'crypto';
import { JsonSchemaToPostgres, IndexType, TableConfig } from './jsonSchemaToPostgres';
import {
  EasyAPIConfig,
  APIModelConfig,
  APIRequest,
  APIResponse,
  APIOperation,
  AuthState,
  PaginationOptions,
  PaginationResult,
  AggregateOptions,
  AggregateField,
  RequestMeta,
  ResponseMeta
} from './types';

// Session type stored in the sessions table
export type Session = {
  token: string;
  userID: string;
  created?: string;
  expires?: string;
};

// Sessions table configuration for authentication
// Uses ONLY indexes on JSONB data column - no generated columns
const sessionsTableConfig: TableConfig = {
  tableName: 'sessions',
  indexes: [
    { type: IndexType.BTREE, fields: ['token'], unique: true },
    { type: IndexType.BTREE, fields: ['userID'] }
  ]
};

/**
 * EasyAPI - A simple API library that creates CRUD endpoints for JSONB-based PostgreSQL tables
 * 
 * Features:
 * - Single POST endpoint that handles all operations
 * - Pre/Post listeners for each operation
 * - JSON Schema validation
 * - Automatic table/index creation on startup
 * - Permission-based access control
 */
export class EasyAPI {
  private pool: Pool;
  private config: EasyAPIConfig;
  private modelMap: Map<string, APIModelConfig> = new Map();
  private router: Router;

  constructor(pool: Pool, config: EasyAPIConfig) {
    this.pool = pool;
    this.config = config;
    this.router = express.Router();

    // Build model lookup map
    for (const model of config.apiModels) {
      this.modelMap.set(model.modelName, model);
    }
  }

  /**
   * Initialize the API - creates tables/indexes and sets up routes
   */
  async initialize(): Promise<Router> {
    // Ensure all tables and indexes exist
    await this.ensureTablesAndIndexes();

    // Set up the single POST endpoint
    this.router.post('/api/clientAPI', async (req: Request, res: Response) => {
      try {
        const result = await this.handleRequest(req.body, req);
        res.status(result.success ? 200 : 400).json(result);
      } catch (error: any) {
        res.status(500).json({
          success: false,
          message: error.message || 'Internal server error',
          data: null,
          isAuthorized: true
        } as APIResponse);
      }
    });

    return this.router;
  }

  /**
   * Ensure all tables and indexes exist based on TableConfig
   */
  private async ensureTablesAndIndexes(): Promise<void> {
    const migrator = new JsonSchemaToPostgres(this.pool);

    // Create internal tables (sessions) for authentication
    try {
      await migrator.migrate(sessionsTableConfig, true);  // debug=true to see SQL
      console.log(`✓ Table 'sessions' ready (internal)`);
    } catch (error: any) {
      console.error(`✗ Failed to create sessions table:`, error.message);
      throw error;
    }

    // Create tables for each API model
    for (const model of this.config.apiModels) {
      if (model.tableConfig) {
        // Ensure table name matches model name
        const tableConfig = {
          ...model.tableConfig,
          tableName: model.modelName
        };
        
        try {
          await migrator.migrate(tableConfig, true);  // debug=true to see SQL
          console.log(`✓ Table '${model.modelName}' ready`);
        } catch (error: any) {
          console.error(`✗ Failed to migrate table '${model.modelName}':`, error.message);
          throw error;
        }
      } else {
        // Create basic table with just data column if no tableConfig
        const basicConfig = {
          tableName: model.modelName
        };
        try {
          await migrator.migrate(basicConfig, true);  // debug=true to see SQL
          console.log(`✓ Table '${model.modelName}' ready (basic)`);
        } catch (error: any) {
          console.error(`✗ Failed to create basic table '${model.modelName}':`, error.message);
          throw error;
        }
      }
    }
  }

  /**
   * Handle an incoming API request
   */
  async handleRequest(apiRequest: APIRequest, req: Request): Promise<APIResponse> {
    const { operation, model, data, query, meta } = apiRequest;
    const pagination = meta?.pagination;

    // Get user from request first to determine isAuthorized for all responses
    const user = await this.getUserFromRequest(req, apiRequest.token);
    const isAuthorized = user !== null;

    // Validate request structure
    if (!operation) {// || !model
      return {
        success: false,
        message: 'Missing required fields: operation',
        data: null,
        isAuthorized
      };
    }

    // Get model config
    const modelConfig = model ? this.modelMap.get(model ?? "") : null;
    /*if (!modelConfig) {
      return {
        success: false,
        message: `Unknown model: ${model}`,
        data: null,
        isAuthorized
      };
    }*/

    // Parse operation
    const op = this.parseOperation(operation);
    if (op === null) {
      return {
        success: false,
        message: `Invalid operation: ${operation}`,
        data: null,
        isAuthorized
      };
    }

    // Check if operation is allowed for this model
    if (modelConfig && !modelConfig.dataOperations.includes(op)) {
      return {
        success: false,
        message: `Operation ${APIOperation[op]} not allowed for model ${model}`,
        data: null,
        isAuthorized
      };
    }

    // Check permissions
    const authRequired = modelConfig ? this.getAuthRequirement(modelConfig, op) : AuthState.OPEN;
    if (authRequired === AuthState.AUTHENTICATED && !user) {
      return {
        success: false,
        message: 'Authentication required',
        data: null,
        isAuthorized
      };
    }

    // Execute the operation
    try {
      switch (op) {
        case APIOperation.READ:
          return modelConfig ? await this.handleRead(modelConfig, user, query, isAuthorized, pagination) : {success: false, message: 'Model not found', data: null, isAuthorized};
        case APIOperation.WRITE:
          return modelConfig ? await this.handleWrite(modelConfig, user, data, isAuthorized) : {success: false, message: 'Model not found', data: null, isAuthorized};
        case APIOperation.UPDATE:
          return modelConfig ? await this.handleUpdate(modelConfig, user, query, data, isAuthorized) : {success: false, message: 'Model not found', data: null, isAuthorized};
        case APIOperation.DELETE:
          return modelConfig ? await this.handleDelete(modelConfig, user, query, isAuthorized) : {success: false, message: 'Model not found', data: null, isAuthorized};
        case APIOperation.LOGIN:
          return await this.handleLogin(data, isAuthorized);
        case APIOperation.LOGOUT:
          return await this.handleLogout(apiRequest.token, isAuthorized);
        case APIOperation.AGGREGATE:
          if (!modelConfig) return { success: false, message: 'Model not found', data: null, isAuthorized };
          if (!apiRequest.aggregate) return { success: false, message: 'aggregate options required', data: null, isAuthorized };
          return {
            success: true,
            data: await this.aggregateData(model!, query, apiRequest.aggregate),
            isAuthorized
          };
        default:
          return {
            success: false,
            message: 'Unknown operation',
            data: null,
            isAuthorized
          };
      }
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Operation failed',
        data: null,
        isAuthorized
      };
    }
  }

  /**
   * Parse operation from string or enum
   */
  private parseOperation(operation: APIOperation | string): APIOperation | null {
    if (typeof operation === 'number') {
      return operation;
    }

    const opMap: Record<string, APIOperation> = {
      'read': APIOperation.READ,
      'READ': APIOperation.READ,
      'write': APIOperation.WRITE,
      'WRITE': APIOperation.WRITE,
      'update': APIOperation.UPDATE,
      'UPDATE': APIOperation.UPDATE,
      'delete': APIOperation.DELETE,
      'DELETE': APIOperation.DELETE,
      'login': APIOperation.LOGIN,
      'LOGIN': APIOperation.LOGIN,
      'logout': APIOperation.LOGOUT,
      'LOGOUT': APIOperation.LOGOUT,
      'aggregate': APIOperation.AGGREGATE,
      'AGGREGATE': APIOperation.AGGREGATE
    };
    
    return opMap[operation] ?? null;
  }

  /**
   * Get auth requirement for an operation
   */
  private getAuthRequirement(model: APIModelConfig, op: APIOperation): AuthState {
    switch (op) {
      case APIOperation.READ:
        return model.permissions.read;
      case APIOperation.WRITE:
        return model.permissions.write;
      case APIOperation.UPDATE:
        return model.permissions.update;
      case APIOperation.DELETE:
        return model.permissions.delete;
      case APIOperation.AGGREGATE:
        return model.permissions.read;
      default:
        return AuthState.AUTHENTICATED;
    }
  }

  /**
   * Check if user is trying to set any readOnly fields
   * Returns array of field names that violate readOnly constraint
   */
  private getReadOnlyViolations(data: any, schema: Record<string, any>): string[] {
    const violations: string[] = [];
    
    if (!schema.properties || !data) {
      return violations;
    }

    for (const [fieldName, fieldSchema] of Object.entries(schema.properties)) {
      if ((fieldSchema as any).readOnly && data.hasOwnProperty(fieldName)) {
        violations.push(fieldName);
      }
    }

    return violations;
  }

  // ============================================================================
  // Password Hashing Utilities
  // ============================================================================

  /**
   * Hash a password with a random salt
   */
  private hashPassword(password: string): { hash: string; salt: string } {
    const salt = crypto.randomBytes(32).toString('hex');
    const hash = crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
    return { hash, salt };
  }

  /**
   * Verify a password against a stored hash and salt
   */
  private verifyPassword(password: string, storedHash: string, salt: string): boolean {
    const hash = crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
    return hash === storedHash;
  }


  // ============================================================================
  // LOGIN Operation
  // ============================================================================

  /**
   * Handle login - verify email/password and create a session
   */
  private async handleLogin(
    data: { email: string; password: string },
    isAuthorized: boolean
  ): Promise<APIResponse> {
    // Ensure userModel is configured
    if (!this.config.userModel) {
      return {
        success: false,
        message: 'User model not configured',
        data: null,
        isAuthorized
      };
    }

    const { email, password } = data || {};

    if (!email || !password) {
      return {
        success: false,
        message: 'Email and password are required',
        data: null,
        isAuthorized
      };
    }

    try {
      // Look up user by email
      const userResult = await this.pool.query(
        `SELECT data FROM ${this.config.userModel} WHERE data->>'email' = $1`,
        [email]
      );

      if (userResult.rows.length === 0) {
        return {
          success: false,
          message: 'Invalid email or password',
          data: null,
          isAuthorized: false
        };
      }

      const userData = userResult.rows[0].data;

      // Verify password
      if (!this.verifyPassword(password, userData.password, userData.salt)) {
        return {
          success: false,
          message: 'Invalid email or password',
          data: null,
          isAuthorized: false
        };
      }

      // Create a new session token
      const token = uuidv7();
      const now = new Date().toISOString();
      const session: Session = {
        token,
        userID: userData.userID,
        created: now
      };

      // Insert session into sessions table
      await this.pool.query(
        'INSERT INTO sessions (data) VALUES ($1)',
        [JSON.stringify(session)]
      );

      // Get user model config to filter non-readable fields
      const userModelConfig = this.modelMap.get(this.config.userModel);
      let responseUser = { ...userData };
      
      // Remove password and salt from response
      delete responseUser.password;
      delete responseUser.salt;
      
      // Also remove any other non-readable fields
      if (userModelConfig?.nonReadableFields) {
        for (const field of userModelConfig.nonReadableFields) {
          delete responseUser[field];
        }
      }

      return {
        success: true,
        message: 'Login successful',
        data: {
          token,
          user: responseUser
        },
        isAuthorized: true
      };
    } catch (error: any) {
      console.error('Login error:', error.message);
      return {
        success: false,
        message: 'Login failed',
        data: null,
        isAuthorized: false
      };
    }
  }

  // ============================================================================
  // LOGOUT Operation
  // ============================================================================

  /**
   * Handle logout - delete the session from the database
   */
  private async handleLogout(token: string | undefined, isAuthorized: boolean): Promise<APIResponse> {
    if (!token) {
      return {
        success: false,
        message: 'No token provided',
        data: null,
        isAuthorized: false
      };
    }

    try {
      // Delete session from sessions table
      await this.pool.query(
        "DELETE FROM sessions WHERE data->>'token' = $1",
        [token]
      );

      return {
        success: true,
        message: 'Logged out successfully',
        data: null,
        isAuthorized: true
      };
    } catch (error: any) {
      console.error('Logout error:', error.message);
      return {
        success: false,
        message: 'Logout failed',
        data: null,
        isAuthorized: false
      };
    }
  }

  /**
   * Get user from token (public method for use in custom routes)
   */
  async getUserFromToken(token: string): Promise<any | null> {
    if (!token || !this.config.userModel) {
      return null;
    }

    try {
      // Look up session by token (query JSONB data column)
      const sessionResult = await this.pool.query(
        "SELECT data FROM sessions WHERE data->>'token' = $1",
        [token]
      );

      if (sessionResult.rows.length === 0) {
        return null;
      }

      const session = sessionResult.rows[0].data as Session;

      // Look up user by userID from the session (query JSONB data column)
      const userResult = await this.pool.query(
        `SELECT data FROM ${this.config.userModel} WHERE data->>'userID' = $1`,
        [session.userID]
      );

      if (userResult.rows.length === 0) {
        return null;
      }

      return userResult.rows[0].data;
    } catch (error: any) {
      console.error('Error fetching user from token:', error.message);
      return null;
    }
  }

  /**
   * Get user from request by looking up the token in the sessions table
   * and then fetching the user from the userModel table
   */
  private async getUserFromRequest(req: Request, token?: string): Promise<any | null> {
    if (!token || !this.config.userModel) {
      return null;
    }

    try {
      // Look up session by token (query JSONB data column)
      const sessionResult = await this.pool.query(
        "SELECT data FROM sessions WHERE data->>'token' = $1",
        [token]
      );

      if (sessionResult.rows.length === 0) {
        return null;
      }

      const session = sessionResult.rows[0].data as Session;

      // Look up user by userID from the session (query JSONB data column)
      const userResult = await this.pool.query(
        `SELECT data FROM ${this.config.userModel} WHERE data->>'userID' = $1`,
        [session.userID]
      );

      if (userResult.rows.length === 0) {
        return null;
      }

      return userResult.rows[0].data;
    } catch (error: any) {
      console.error('Error fetching user from session:', error.message);
      return null;
    }
  }

  // ============================================================================
  // READ Operation
  // ============================================================================

  private async handleRead(
    model: APIModelConfig,
    user: any | null,
    query: any,
    isAuthorized: boolean,
    pagination?: PaginationOptions
  ): Promise<APIResponse> {
    // Ensure query is an object so listeners can mutate it
    query = query || {};

    // Run pre-read listeners
    if (model.readPreListeners) {
      for (const listener of model.readPreListeners) {
        const result = await listener(user!, query);
        if (!result.success) {
          console.log(`[READ ${model.modelName}] Pre-read listener failed:`, result.message);
          return {
            success: false,
            message: result.message || 'Pre-read listener failed',
            data: null,
            isAuthorized
          };
        }
      }
    }

    // Build and execute query with pagination
    const { data: rawData, total } = await this.executeSelectPaginated(model.modelName, query, pagination);
    let data = rawData;

    // Run post-read listeners
    if (model.readPostListeners) {
      for (const listener of model.readPostListeners) {
        const listenerResult = await listener(user!, query, data);
        if (!listenerResult.success) {
          return {
            success: false,
            message: listenerResult.message || 'Post-read listener failed',
            data: null,
            isAuthorized
          };
        }
      }
    }

    // Filter out non-readable fields
    if (model.nonReadableFields && model.nonReadableFields.length > 0) {
      data = data.map(item => {
        const filtered = { ...item };
        for (const field of model.nonReadableFields!) {
          delete filtered[field];
        }
        return filtered;
      });
    }

    // Build pagination result if pagination was requested
    const paginationResult: PaginationResult | undefined = pagination ? {
      total,
      limit: pagination.limit || 50,
      offset: pagination.offset || 0,
      hasMore: (pagination.offset || 0) + data.length < total
    } : undefined;

    // Build response meta if we have pagination
    const meta: ResponseMeta | undefined = paginationResult ? { pagination: paginationResult } : undefined;

    return {
      success: true,
      data,
      isAuthorized,
      meta
    };
  }

  // ============================================================================
  // WRITE Operation
  // ============================================================================

  private async handleWrite(
    model: APIModelConfig,
    user: any | null,
    data: any,
    isAuthorized: boolean
  ): Promise<APIResponse> {
    
    // Validate against JSON schema (after adding auto-generated fields)
    if (model.jsonSchema) {
      const validation = validate(data, model.jsonSchema);
      if (validation.errors.length > 0) {
        const errorMessages = validation.errors.map((e: any) => `${e.property} ${e.message}`).join('; ');
        return {
          success: false,
          message: `Validation failed: ${errorMessages}`,
          data: null,
          isAuthorized
        };
      }
    }
    // Check for readOnly fields that user is trying to set
    if (model.jsonSchema) {
      const readOnlyViolations = this.getReadOnlyViolations(data, model.jsonSchema);
      if (readOnlyViolations.length > 0) {
        return {
          success: false,
          message: `Cannot set readOnly fields: ${readOnlyViolations.join(', ')}`,
          data: null,
          isAuthorized
        };
      }
    }

    // Auto-generate primary key (UUIDv7), created, and updated timestamps
    const now = new Date().toISOString();
    let enrichedData = {
      ...data,
      [model.primaryKey]: uuidv7(),  // Generate UUIDv7 for primary key
      created: now,
      updated: now
    };

    // If this is the user model and password is provided, hash it
    if (this.config.userModel && model.modelName === this.config.userModel && enrichedData.password) {
      const { hash, salt } = this.hashPassword(enrichedData.password);
      enrichedData.password = hash;
      enrichedData.salt = salt;
    }

    

    // Run pre-write listeners
    if (model.writePreListeners) {
      for (const listener of model.writePreListeners) {
        const result = await listener(user!, enrichedData);
        if (!result.success) {
          return {
            success: false,
            message: result.message || 'Pre-write listener failed',
            data: null,
            isAuthorized
          };
        }
      }
    }

    // Insert into database
    const sql = `INSERT INTO ${model.modelName} (data) VALUES ($1) RETURNING data`;
    const result = await this.pool.query(sql, [JSON.stringify(enrichedData)]);
    let insertedData = result.rows[0]?.data;

    // Run post-write listeners
    if (model.writePostListeners) {
      for (const listener of model.writePostListeners) {
        const listenerResult = await listener(user!, insertedData);
        if (!listenerResult.success) {
          return {
            success: false,
            message: listenerResult.message || 'Post-write listener failed',
            data: insertedData,
            isAuthorized
          };
        }
      }
    }

    // Filter out non-readable fields from response
    if (model.nonReadableFields && model.nonReadableFields.length > 0 && insertedData) {
      insertedData = { ...insertedData };
      for (const field of model.nonReadableFields) {
        delete insertedData[field];
      }
    }

    return {
      success: true,
      data: insertedData,
      isAuthorized
    };
  }

  // ============================================================================
  // UPDATE Operation
  // ============================================================================

  private async handleUpdate(
    model: APIModelConfig,
    user: any | null,
    query: any,
    data: any,
    isAuthorized: boolean
  ): Promise<APIResponse> {
    if (!query || Object.keys(query).length === 0) {
      return {
        success: false,
        message: 'Update requires a query to identify records',
        data: null,
        isAuthorized
      };
    }

    // Ensure query contains the primary key
    if (!query.hasOwnProperty(model.primaryKey)) {
      return {
        success: false,
        message: `Update query must include primary key: ${model.primaryKey}`,
        data: null,
        isAuthorized
      };
    }
    // Check for readOnly fields that user is trying to update
    if (model.jsonSchema) {
      const readOnlyViolations = this.getReadOnlyViolations(data, model.jsonSchema);
      if (readOnlyViolations.length > 0) {
        return {
          success: false,
          message: `Cannot update readOnly fields: ${readOnlyViolations.join(', ')}`,
          data: null,
          isAuthorized
        };
      }
    }

    // Auto-update the 'updated' timestamp
    const enrichedData = {
      ...data,
      updated: new Date().toISOString()
    };

    // Validate update data against JSON schema (partial validation)
    // Note: For updates, we might want to allow partial objects
    
    // Run pre-update listeners
    if (model.updatePreListeners) {
      for (const listener of model.updatePreListeners) {
        const result = await listener(user!, query, enrichedData);
        if (!result.success) {
          return {
            success: false,
            message: result.message || 'Pre-update listener failed',
            data: null,
            isAuthorized
          };
        }
      }
    }

    // Build and execute update query
    // This merges the new data with existing data using jsonb_concat (||)
    const { whereClause, params } = this.buildWhereClause(query, 2);  // Start at $2 since $1 is the JSON data
    const sql = `
      UPDATE ${model.modelName} 
      SET data = data || $1::jsonb
      WHERE ${whereClause}
      RETURNING data
    `;
    
    const result = await this.pool.query(sql, [JSON.stringify(enrichedData), ...params]);
    const updatedData = result.rows.map(row => row.data);

    // Run post-update listeners
    if (model.updatePostListeners) {
      for (const listener of model.updatePostListeners) {
        const listenerResult = await listener(user!, query, updatedData);
        if (!listenerResult.success) {
          return {
            success: false,
            message: listenerResult.message || 'Post-update listener failed',
            data: updatedData,
            isAuthorized
          };
        }
      }
    }

    return {
      success: true,
      message: `Updated ${result.rowCount} record(s)`,
      data: updatedData,
      isAuthorized
    };
  }

  // ============================================================================
  // DELETE Operation
  // ============================================================================

  private async handleDelete(
    model: APIModelConfig,
    user: any | null,
    query: any,
    isAuthorized: boolean
  ): Promise<APIResponse> {
    if (!query || Object.keys(query).length === 0) {
      return {
        success: false,
        message: 'Delete requires a query to identify records',
        data: null,
        isAuthorized
      };
    }

    // Run pre-delete listeners
    if (model.deletePreListeners) {
      for (const listener of model.deletePreListeners) {
        const result = await listener(user!, query);
        if (!result.success) {
          return {
            success: false,
            message: result.message || 'Pre-delete listener failed',
            data: null,
            isAuthorized
          };
        }
      }
    }

    // Build and execute delete query
    const { whereClause, params } = this.buildWhereClause(query);
    const sql = `DELETE FROM ${model.modelName} WHERE ${whereClause} RETURNING data`;
    
    const result = await this.pool.query(sql, params);
    const deletedData = result.rows.map(row => row.data);

    // Run post-delete listeners
    if (model.deletePostListeners) {
      for (const listener of model.deletePostListeners) {
        const listenerResult = await listener(user!, query);
        if (!listenerResult.success) {
          return {
            success: false,
            message: listenerResult.message || 'Post-delete listener failed',
            data: deletedData,
            isAuthorized
          };
        }
      }
    }

    return {
      success: true,
      message: `Deleted ${result.rowCount} record(s)`,
      data: deletedData,
      isAuthorized
    };
  }

  // ============================================================================
  // Query Building Helpers
  // ============================================================================

  /**
   * Execute a SELECT query and return the data from JSONB column
   * 
   * @param tableName - The table to query
   * @param query - Optional query object for filtering
   * @returns Array of data objects from the JSONB column
   */
  private async executeSelect(tableName: string, query?: any): Promise<any[]> {
    const { sql, params } = this.buildSelectQuery(tableName, query);
    const result = await this.pool.query(sql, params);
    return result.rows.map(row => row.data);
  }

  /**
   * Execute SELECT with pagination support
   * Returns data and total count for pagination metadata
   */
  private async executeSelectPaginated(
    tableName: string, 
    query?: any, 
    pagination?: PaginationOptions
  ): Promise<{ data: any[]; total: number }> {
    // Apply defaults and limits
    const limit = Math.min(pagination?.limit || 50, 1000);  // Max 1000
    const offset = pagination?.offset || 0;
    const orderBy = pagination?.orderBy || 'created';
    const orderDirection = pagination?.orderDirection || 'DESC';

    // Build base query parts
    const { whereClause, params } = query && Object.keys(query).length > 0
      ? this.buildWhereClause(query)
      : { whereClause: 'TRUE', params: [] };

    // Get total count first
    const countSql = `SELECT COUNT(*) as total FROM ${tableName} WHERE ${whereClause}`;
    const countResult = await this.pool.query(countSql, params);
    const total = parseInt(countResult.rows[0]?.total || '0', 10);

    // Build paginated data query
    const orderPath = this.buildJsonPath(orderBy);
    const dataSql = `
      SELECT data FROM ${tableName} 
      WHERE ${whereClause}
      ORDER BY ${orderPath} ${orderDirection}
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;
    const dataParams = [...params, limit, offset];
    const dataResult = await this.pool.query(dataSql, dataParams);

    return {
      data: dataResult.rows.map(row => row.data),
      total
    };
  }

  /**
   * Build a SELECT query from a query object
   * Query object keys are JSONB field paths, values are the match values
   * 
   * Example query: { clientID: 'abc123', status: 'active' }
   * Generates: SELECT data FROM table WHERE data->>'clientID' = $1 AND data->>'status' = $2
   */
  private buildSelectQuery(tableName: string, query?: any): { sql: string; params: any[] } {
    if (!query || Object.keys(query).length === 0) {
      return {
        sql: `SELECT data FROM ${tableName}`,
        params: []
      };
    }

    const { whereClause, params } = this.buildWhereClause(query);
    return {
      sql: `SELECT data FROM ${tableName} WHERE ${whereClause}`,
      params
    };
  }

  /**
   * Build WHERE clause from query object
   * Supports:
   * - Simple equality: { field: value }
   * - Nested fields: { 'address.city': 'NYC' }
   * - Operators: { field: { $gt: 5, $lt: 10 } }
   * @param startParamIndex - Starting index for parameter placeholders (default 1)
   */
  private buildWhereClause(query: any, startParamIndex: number = 1): { whereClause: string; params: any[] } {
    const conditions: string[] = [];
    const params: any[] = [];
    let paramIndex = startParamIndex;

    for (const [field, value] of Object.entries(query)) {
      // Handle $or operator
      if (field === '$or' && Array.isArray(value)) {
        const orConditions: string[] = [];
        for (const subQuery of value) {
          const subResult = this.buildWhereClause(subQuery, paramIndex);
          if (subResult.whereClause !== 'TRUE') {
            orConditions.push(`(${subResult.whereClause})`);
            params.push(...subResult.params);
            paramIndex += subResult.params.length;
          }
        }
        if (orConditions.length > 0) {
          conditions.push(`(${orConditions.join(' OR ')})`);
        }
        continue;
      }

      if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        // Handle operators
        for (const [op, opValue] of Object.entries(value as Record<string, any>)) {
          const jsonPath = this.buildJsonPath(field);
          switch (op) {
            case '$gt':
              conditions.push(`${jsonPath} > $${paramIndex}`);
              params.push(opValue);
              paramIndex++;
              break;
            case '$gte':
              conditions.push(`${jsonPath} >= $${paramIndex}`);
              params.push(opValue);
              paramIndex++;
              break;
            case '$lt':
              conditions.push(`${jsonPath} < $${paramIndex}`);
              params.push(opValue);
              paramIndex++;
              break;
            case '$lte':
              conditions.push(`${jsonPath} <= $${paramIndex}`);
              params.push(opValue);
              paramIndex++;
              break;
            case '$ne':
              conditions.push(`${jsonPath} != $${paramIndex}`);
              params.push(opValue);
              paramIndex++;
              break;
            case '$in':
              conditions.push(`${jsonPath} = ANY($${paramIndex})`);
              params.push(opValue);
              paramIndex++;
              break;
            case '$contains':
              // For JSONB containment
              conditions.push(`data @> $${paramIndex}::jsonb`);
              params.push(JSON.stringify({ [field]: opValue }));
              paramIndex++;
              break;
          }
        }
      } else {
        // Simple equality
        const jsonPath = this.buildJsonPath(field);
        conditions.push(`${jsonPath} = $${paramIndex}`);
        params.push(value);
        paramIndex++;
      }
    }

    return {
      whereClause: conditions.length > 0 ? conditions.join(' AND ') : 'TRUE',
      params
    };
  }

  /**
   * Build JSON path expression for PostgreSQL
   * 'clientID' -> data->>'clientID'
   * 'address.city' -> data->'address'->>'city'
   */
  private buildJsonPath(field: string): string {
    const parts = field.split('.');
    if (parts.length === 1) {
      return `data->>'${parts[0]}'`;
    }
    
    const lastPart = parts.pop()!;
    const middleParts = parts.map(p => `data->'${p}'`).join('->');
    return `${middleParts}->>'${lastPart}'`;
  }

  /**
   * Get the Express router
   */
  getRouter(): Router {
    return this.router;
  }

  /**
   * Server-side helper to WRITE data without going through HTTP
   */
  async writeData(modelName: string, data: any, user?: any | null): Promise<any> {
    const model = this.modelMap.get(modelName);
    if (!model) {
      throw new Error(`Unknown model: ${modelName}`);
    }

    const result = await this.handleWrite(model, user ?? null, data, true);
    if (!result.success) {
      throw new Error(result.message || 'Write operation failed');
    }

    return result.data;
  }

  /**
   * Server-side helper to READ data without going through HTTP
   */
  async readData(modelName: string, query?: any, user?: any | null): Promise<any[]> {
    const model = this.modelMap.get(modelName);
    if (!model) {
      throw new Error(`Unknown model: ${modelName}`);
    }

    const result = await this.handleRead(model, user ?? null, query, true);
    if (!result.success) {
      throw new Error(result.message || 'Read operation failed');
    }

    return (result.data as any[]) || [];
  }

  /**
   * Server-side helper to READ data with pagination
   * Returns { data, pagination } with total count and hasMore flag
   */
  async readDataPaginated(
    modelName: string, 
    query?: any, 
    pagination?: PaginationOptions,
    user?: any | null
  ): Promise<{ data: any[]; pagination: PaginationResult }> {
    const model = this.modelMap.get(modelName);
    if (!model) {
      throw new Error(`Unknown model: ${modelName}`);
    }

    const result = await this.handleRead(model, user ?? null, query, true, pagination || {});
    if (!result.success) {
      throw new Error(result.message || 'Read operation failed');
    }

    return {
      data: (result.data as any[]) || [],
      pagination: result.meta!.pagination!
    };
  }

  /**
   * Server-side helper to UPDATE data without going through HTTP
   */
  async updateData(modelName: string, query: any, data: any, user?: any | null): Promise<any> {
    const model = this.modelMap.get(modelName);
    if (!model) {
      throw new Error(`Unknown model: ${modelName}`);
    }

    const result = await this.handleUpdate(model, user ?? null, query, data, true);
    if (!result.success) {
      throw new Error(result.message || 'Update operation failed');
    }

    return result.data;
  }

  /**
   * Server-side helper to DELETE data without going through HTTP
   */
  async deleteData(modelName: string, query: any, user?: any | null): Promise<any> {
    const model = this.modelMap.get(modelName);
    if (!model) {
      throw new Error(`Unknown model: ${modelName}`);
    }

    const result = await this.handleDelete(model, user ?? null, query, true);
    if (!result.success) {
      throw new Error(result.message || 'Delete operation failed');
    }

    return result.data;
  }

  /**
   * Server-side helper to run aggregate queries (GROUP BY, COUNT, MIN, MAX, etc.)
   *
   * Usage:
   *   const rows = await easyAPI.aggregateData('market_data',
   *     { ticker: 'SPY' },                       // optional WHERE filter
   *     {
   *       groupBy: ['ticker'],
   *       aggregates: [
   *         { field: 'timestamp', function: 'min', alias: 'earliest' },
   *         { field: 'timestamp', function: 'max', alias: 'latest' },
   *         { field: '*',         function: 'count', alias: 'records' },
   *       ],
   *       orderBy: 'ticker',
   *     }
   *   );
   */
  async aggregateData(
    modelName: string,
    query: any | undefined,
    options: AggregateOptions
  ): Promise<any[]> {
    const model = this.modelMap.get(modelName);
    if (!model) {
      throw new Error(`Unknown model: ${modelName}`);
    }

    // --- SELECT columns ---
    const selectParts: string[] = [];

    // Group-by fields
    for (const field of options.groupBy) {
      const path = this.buildJsonPath(field);
      selectParts.push(`${path} AS "${field}"`);
    }

    // Aggregate expressions
    for (const agg of options.aggregates) {
      const expr = this.buildAggregateExpression(agg);
      selectParts.push(`${expr} AS "${agg.alias}"`);
    }

    // --- WHERE clause ---
    const { whereClause, params } = query && Object.keys(query).length > 0
      ? this.buildWhereClause(query)
      : { whereClause: 'TRUE', params: [] as any[] };

    // --- GROUP BY ---
    const groupByParts = options.groupBy.map(f => this.buildJsonPath(f));

    // --- ORDER BY ---
    let orderByExpr = groupByParts[0] || '1';
    if (options.orderBy) {
      // Check if it matches a groupBy field
      if (options.groupBy.includes(options.orderBy)) {
        orderByExpr = this.buildJsonPath(options.orderBy);
      } else {
        // Might be an alias – reference it directly
        orderByExpr = `"${options.orderBy}"`;
      }
    }
    const direction = options.orderDirection || 'ASC';

    const sql = [
      `SELECT ${selectParts.join(', ')}`,
      `FROM ${modelName}`,
      `WHERE ${whereClause}`,
      `GROUP BY ${groupByParts.join(', ')}`,
      `ORDER BY ${orderByExpr} ${direction}`,
    ].join(' ');

    const result = await this.pool.query(sql, params);
    return result.rows;
  }

  /**
   * Build a SQL aggregate expression from an AggregateField spec
   */
  private buildAggregateExpression(agg: AggregateField): string {
    const path = agg.field === '*' ? '*' : this.buildJsonPath(agg.field);

    switch (agg.function) {
      case 'count':
        return `COUNT(${path})`;
      case 'countDistinct':
        return `COUNT(DISTINCT ${path})`;
      case 'min':
        return `MIN(${path})`;
      case 'max':
        return `MAX(${path})`;
      case 'sum':
        return `SUM((${path})::numeric)`;
      case 'avg':
        return `AVG((${path})::numeric)`;
      default:
        throw new Error(`Unsupported aggregate function: ${agg.function}`);
    }
  }
}

// ============================================================================
// Factory function for easy setup
// ============================================================================

/**
 * Create and initialize an EasyAPI instance
 * 
 * Usage:
 * const api = await createEasyAPI(pool, config);
 * app.use(api.getRouter());
 */
export async function createEasyAPI(pool: Pool, config: EasyAPIConfig): Promise<EasyAPI> {
  const api = new EasyAPI(pool, config);
  await api.initialize();
  return api;
}

export default EasyAPI;
