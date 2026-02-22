import { IndexType, TableConfig } from "./jsonSchemaToPostgres";

export enum APIOperation {
    WRITE,
    READ,
    UPDATE,
    DELETE,
    LOGIN,
    LOGOUT,
    AGGREGATE
}

export enum AuthState {
    AUTHENTICATED,
    OPEN
}

export type ListenerResponse = {
    success: boolean;
    message?: string;
};

export type APIResponse = {
    success: boolean;
    message?: string;
    data: any;
    isAuthorized: boolean;
    meta?: ResponseMeta;
};

/**
 * Response metadata (extensible for future needs)
 */
export type ResponseMeta = {
    pagination?: PaginationResult;
};

/**
 * Request metadata (extensible for future needs)
 */
export type RequestMeta = {
    pagination?: PaginationOptions;
};

/**
 * Pagination options for read queries
 */
export type PaginationOptions = {
    limit?: number;       // Max records to return (default: 50, max: 1000)
    offset?: number;      // Number of records to skip (default: 0)
    orderBy?: string;     // Field to order by (default: 'created')
    orderDirection?: 'ASC' | 'DESC';  // Sort direction (default: 'DESC')
};

/**
 * Pagination result metadata
 */
export type PaginationResult = {
    total: number;        // Total records matching query
    limit: number;        // Records per page
    offset: number;       // Current offset
    hasMore: boolean;     // Whether more records exist
};

/**
 * Aggregate field specification
 * Defines what aggregate function to apply to which field
 */
export type AggregateField = {
    field: string;        // JSONB field path (e.g. 'timestamp', 'ticker')
    function: 'count' | 'countDistinct' | 'min' | 'max' | 'sum' | 'avg';
    alias: string;        // Name for the result column
};

/**
 * Options for aggregate queries
 */
export type AggregateOptions = {
    groupBy: string[];           // Fields to group by
    aggregates: AggregateField[]; // Aggregate functions to apply
    orderBy?: string;            // Field or alias to order by
    orderDirection?: 'ASC' | 'DESC';
};

export type APIRequest = {
    operation?: APIOperation | string;
    model?: string;
    data?: any;
    query?: any;
    token?: string;
    meta?: RequestMeta;
    aggregate?: AggregateOptions;
};

export type DataPreReadListener = (user: any, query: any) => Promise<ListenerResponse>;
export type DataPostReadListener = (user: any, query: any, result: any) => Promise<ListenerResponse>;

export type DataPreWriteListener = (user: any, data: any) => Promise<ListenerResponse>;
export type DataPostWriteListener = (user: any, data: any) => Promise<ListenerResponse>;

export type DataPreUpdateListener = (user: any, query: any, data: any) => Promise<ListenerResponse>;
export type DataPostUpdateListener = (user: any, query: any, data: any) => Promise<ListenerResponse>;
export type DataPreDeleteListener = (user: any, query: any) => Promise<ListenerResponse>;
export type DataPostDeleteListener = (user: any, query: any) => Promise<ListenerResponse>;

export const testListener: DataPreWriteListener = async (user: any, data: any): Promise<ListenerResponse> => {
    // Perform some operations with user and data
    return {
        success: true,
        message: "Test listener executed successfully"
    };
}

export type EasyAPIConfig = {
    apiModels: APIModelConfig[];
    userModel: string;  // The model name that stores users (e.g., 'users')
};

export const conversationConfig: TableConfig = {
  tableName: 'conversations',

  // Extract frequently-queried fields into generated columns for indexing
  indexedFields: [
    { jsonPath: 'conversationID', columnName: 'conversationID', pgType: 'TEXT' },
    { jsonPath: 'clientID', columnName: 'clientID', pgType: 'TEXT' }
  ],

  // Indexes for fast lookups
  indexes: [
    { type: IndexType.BTREE, columns: ['conversationID'], unique: true },
    { type: IndexType.BTREE, columns: ['clientID'] },
    { type: IndexType.BTREE, fields: ['created'] },
    { type: IndexType.BTREE, fields: ['updated'] },
    // GIN index on dataCollected for flexible querying
    { type: IndexType.GIN, fields: ['dataCollected'] }
  ]
};


//need to enforce userID and clientID logic so that cant be spoofed
export type APIModelConfig = {
    modelName: string;
    primaryKey: string;
    permissions: {
        read: AuthState;
        write: AuthState;
        update: AuthState;
        delete: AuthState;
    };
    jsonSchema: Record<string, any>;
    dataOperations: APIOperation[];
    nonReadableFields?: string[];
    readPreListeners?: DataPreReadListener[];
    readPostListeners?: DataPostReadListener[];
    writePreListeners?: DataPreWriteListener[];
    writePostListeners?: DataPostWriteListener[];
    updatePreListeners?: DataPreUpdateListener[];
    updatePostListeners?: DataPostUpdateListener[];
    deletePreListeners?: DataPreDeleteListener[];
    deletePostListeners?: DataPostDeleteListener[];
    tableConfig?: TableConfig;
}

