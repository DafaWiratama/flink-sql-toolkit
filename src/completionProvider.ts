import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { Logger } from './utils/logger';

export class FlinkSqlCompletionItemProvider implements vscode.CompletionItemProvider {

    private client: FlinkGatewayClient;
    private sessionManager: SessionManager;

    // Cache for metadata
    // Key: 'catalogs' -> list of catalogs
    // Key: 'dbs:catalogName' -> list of databases
    // Key: 'tables:catalogName.dbName' -> list of tables
    private metadataCache: Map<string, string[]> = new Map();

    constructor(client: FlinkGatewayClient, sessionManager: SessionManager) {
        this.client = client;
        this.sessionManager = sessionManager;
    }

    public updateClient(client: FlinkGatewayClient) {
        this.client = client;
        this.clearCache();
    }

    public clearCache() {
        this.metadataCache.clear();
    }

    private ddlKeywords: string[] = [
        'CREATE', 'DROP', 'ALTER', 'TABLE', 'VIEW', 'DATABASE', 'CATALOG', 'FUNCTION',
        'TEMPORARY', 'SYSTEM', 'IF EXISTS', 'IF NOT EXISTS', 'COMMENT', 'PARTITIONED BY',
        'WITH', 'LIKE', 'AS'
    ];

    private dmlKeywords: string[] = [
        'SELECT', 'INSERT INTO', 'INSERT OVERWRITE', 'UPDATE', 'DELETE', 'MERGE INTO',
        'VALUES', 'DISTINCT', 'ALL'
    ];

    private sqlKeywords: string[] = [
        'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET',
        'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN',
        'ON', 'USING', 'UNION', 'INTERSECT', 'EXCEPT', 'IN', 'EXISTS', 'BETWEEN',
        'AND', 'OR', 'NOT', 'IS NULL', 'IS NOT NULL', 'CASE', 'WHEN', 'THEN',
        'ELSE', 'END', 'CAST', 'TRY_CAST', 'LATERAL', 'UNNEST', 'TABLE'
    ];

    private functions: string[] = [
        // Aggregations
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COLLECT', 'LISTAGG',
        // String
        'SUBSTRING', 'CHAR_LENGTH', 'UPPER', 'LOWER', 'TRIM', 'REGEXP_REPLACE',
        'CONCAT', 'CONCAT_WS', 'INITCAP', 'LIKE',
        // Temporal
        'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME', 'LOCALTIMESTAMP',
        'DATE_FORMAT', 'TIMESTAMPADD', 'TIMESTAMPDIFF', 'EXTRACT',
        'FLOOR', 'CEIL',
        // Window TVF
        'TUMBLE', 'HOP', 'SESSION', 'CUMULATE',
        // Conditional
        'COALESCE', 'NULLIF', 'IF',
        // Collection
        'ARRAY', 'MAP', 'ROW'
    ];

    public async provideCompletionItems(
        document: vscode.TextDocument,
        position: vscode.Position,
        token: vscode.CancellationToken,
        context: vscode.CompletionContext
    ): Promise<vscode.CompletionItem[]> {

        const range = document.getWordRangeAtPosition(position);
        const linePrefix = document.lineAt(position).text.substr(0, position.character);

        const allItems: vscode.CompletionItem[] = [];

        // 1. Keyword Completion (always available, but lower priority if inside dot notation)
        if (!linePrefix.trim().endsWith('.')) {
            this.addItems(allItems, this.ddlKeywords, vscode.CompletionItemKind.Keyword);
            this.addItems(allItems, this.dmlKeywords, vscode.CompletionItemKind.Keyword);
            this.addItems(allItems, this.sqlKeywords, vscode.CompletionItemKind.Keyword);
            this.addItems(allItems, this.functions, vscode.CompletionItemKind.Function);
        }

        // 2. Metadata Completion (Dynamic)
        try {
            await this.addMetadataItems(linePrefix, allItems);
        } catch (e) {
            Logger.warn('Metadata completion failed:', e);
        }

        return allItems;
    }

    private addItems(bucket: vscode.CompletionItem[], labels: string[], kind: vscode.CompletionItemKind) {
        for (const label of labels) {
            const item = new vscode.CompletionItem(label, kind);
            bucket.push(item);
        }
    }

    private async addMetadataItems(linePrefix: string, bucket: vscode.CompletionItem[]) {
        // Regex to detect "catalog." or "catalog.db." context
        // Matches: "some_catalog." or "some_catalog.some_db."
        const dotMatch = linePrefix.match(/([a-zA-Z0-9_]+)\.$/);
        const doubleDotMatch = linePrefix.match(/([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.$/);

        const sessionHandle = await this.sessionManager.getActiveSessionHandle();
        if (!sessionHandle) { return; }

        if (doubleDotMatch) {
            // Case: catalog.database. -> List Tables
            const catalog = doubleDotMatch[1];
            const db = doubleDotMatch[2];
            const tables = await this.getTables(sessionHandle, catalog, db);
            this.addItems(bucket, tables, vscode.CompletionItemKind.Class); // Use Class icon for tables
        } else if (dotMatch) {
            // Case: catalog. -> List Databases
            // OR Case: database. -> List Tables (if implicit default catalog)
            // Ideally we check if the prefix is a valid catalog.

            const potentialCatalog = dotMatch[1];
            const catalogs = await this.getCatalogs(sessionHandle);

            if (catalogs.includes(potentialCatalog)) {
                // It IS a catalog -> show databases
                const dbs = await this.getDatabases(sessionHandle, potentialCatalog);
                this.addItems(bucket, dbs, vscode.CompletionItemKind.Module); // Use Module icon for DBs
            } else {
                // It might be a database in the current catalog? 
                // For now, let's keep it simple: strict catalog.db.table hierarchy or just global suggestions?
                // Let's suggest Databases assuming it was a Catalog, AND suggest Tables assuming it was a DB in current catalog.
                // Getting current catalog is expensive (SHOW CURRENT CATALOG), so we might just loop locally if we tracked it.
                // For safety, let's just assume strict catalog.db structure for dot completion to avoid noise,
                // OR we can fetch databases for the 'default_catalog' (usually exists) matching this name.
            }

        } else {
            // No dot -> Just typing a name.

            // Suggest Catalogs
            const catalogs = await this.getCatalogs(sessionHandle);
            this.addItems(bucket, catalogs, vscode.CompletionItemKind.Folder); // Use Folder icon for Catalogs

            // Suggest Tables from current context (default catalog/db)
            // This prioritizes object names as requested
            try {
                const tables = await this.getTablesCurrent(sessionHandle);
                this.addItems(bucket, tables, vscode.CompletionItemKind.Class);
            } catch (e) {
                // Ignore if fails
            }
        }
    }

    // --- Cached Fetchers ---

    private async getCatalogs(sessionHandle: string): Promise<string[]> {
        if (this.metadataCache.has('catalogs')) {
            return this.metadataCache.get('catalogs')!;
        }
        try {
            const result = await this.client.listCatalogs(sessionHandle);
            this.metadataCache.set('catalogs', result);
            return result;
        } catch (e) {
            return [];
        }
    }

    private async getDatabases(sessionHandle: string, catalog: string): Promise<string[]> {
        const key = `dbs:${catalog}`;
        if (this.metadataCache.has(key)) {
            return this.metadataCache.get(key)!;
        }
        try {
            const result = await this.client.listDatabases(sessionHandle, catalog);
            this.metadataCache.set(key, result);
            return result;
        } catch (e) {
            return [];
        }
    }

    private async getTables(sessionHandle: string, catalog: string, db: string): Promise<string[]> {
        const key = `tables:${catalog}.${db}`;
        if (this.metadataCache.has(key)) {
            return this.metadataCache.get(key)!;
        }
        try {
            const result = await this.client.listTables(sessionHandle, catalog, db);
            this.metadataCache.set(key, result);
            return result;
        } catch (e) {
            return [];
        }
    }

    private async getTablesCurrent(sessionHandle: string): Promise<string[]> {
        const key = `tables:current`;
        if (this.metadataCache.has(key)) {
            return this.metadataCache.get(key)!;
        }
        try {
            const result = await this.client.listTablesCurrent(sessionHandle);
            this.metadataCache.set(key, result);
            return result;
        } catch (e) {
            return [];
        }
    }
}
