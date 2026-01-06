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
    // Key: 'columns:catalogName.dbName.tableName' -> list of columns
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

        const linePrefix = document.lineAt(position).text.substr(0, position.character);
        const fullText = document.getText();
        const offset = document.offsetAt(position);
        const textBefore = fullText.substring(0, offset);

        const allItems: vscode.CompletionItem[] = [];

        // 1. Keyword Completion (always available, but lower priority if inside dot notation)
        if (!linePrefix.trim().endsWith('.')) {
            this.addItems(allItems, this.ddlKeywords, vscode.CompletionItemKind.Keyword);
            this.addItems(allItems, this.dmlKeywords, vscode.CompletionItemKind.Keyword);
            this.addItems(allItems, this.sqlKeywords, vscode.CompletionItemKind.Keyword);
            this.addItems(allItems, this.functions, vscode.CompletionItemKind.Function);
        }

        // 2. Metadata & Column Completion
        try {
            await this.addMetadataItems(linePrefix, allItems, fullText);
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

    private async addMetadataItems(linePrefix: string, bucket: vscode.CompletionItem[], sqlText: string) {
        const sessionHandle = await this.sessionManager.getActiveSessionHandle();
        if (!sessionHandle) { return; }

        // Analyze Context for Tables/Aliases
        const contextTables = this.extractTableReferences(sqlText);

        // Regex to detect "identifier." or "identifier.identifier." context
        // Matches: "some_catalog." or "some_catalog.some_db."
        // We use a looser regex that allows backticks and hyphens
        const dotMatch = linePrefix.match(/([`a-zA-Z0-9_\-]+)\.$/);
        const doubleDotMatch = linePrefix.match(/([`a-zA-Z0-9_\-]+)\.([`a-zA-Z0-9_\-]+)\.$/);

        if (doubleDotMatch) {
            // Case: catalog.database. -> List Tables
            const p1 = this.stripQuotes(doubleDotMatch[1]);
            const p2 = this.stripQuotes(doubleDotMatch[2]);
            const tables = await this.getTables(sessionHandle, p1, p2);
            this.addItems(bucket, tables, vscode.CompletionItemKind.Class);
        } else if (dotMatch) {
            // Case: identifier.
            const identifier = this.stripQuotes(dotMatch[1]);
            let foundSomething = false;

            // Check if matches a known alias or table
            for (const ref of contextTables) {
                if (ref.alias === identifier || ref.tableName === identifier) {
                    // It's a table look up!
                    const cols = await this.getColumnsGeneric(sessionHandle, ref.fullPath);
                    this.addItems(bucket, cols, vscode.CompletionItemKind.Field);
                    foundSomething = true;
                }
            }

            // Also check if it's a Catalog
            const catalogs = await this.getCatalogs(sessionHandle);
            if (catalogs.includes(identifier)) {
                // It IS a catalog -> show databases
                const dbs = await this.getDatabases(sessionHandle, identifier);
                this.addItems(bucket, dbs, vscode.CompletionItemKind.Module);
                foundSomething = true;
            }

        } else {
            // No dot -> Just typing a name.

            // Suggest Catalogs
            const catalogs = await this.getCatalogs(sessionHandle);
            this.addItems(bucket, catalogs, vscode.CompletionItemKind.Folder);

            // Suggest Tables from current context
            try {
                const tables = await this.getTablesCurrent(sessionHandle);
                this.addItems(bucket, tables, vscode.CompletionItemKind.Class);
            } catch (e) {
                // Ignore
            }

            // Suggest Columns from ALL context tables (unqualified)
            for (const ref of contextTables) {
                const cols = await this.getColumnsGeneric(sessionHandle, ref.fullPath);
                this.addItems(bucket, cols, vscode.CompletionItemKind.Field);
            }
        }
    }

    private extractTableReferences(text: string): { tableName: string, alias: string, fullPath: string }[] {
        const refs: { tableName: string, alias: string, fullPath: string }[] = [];

        // Regex to find tables in FROM/JOIN. 
        // Supports: `cat`.`db`.`tbl` AS `alias`
        // Groups: 1=FullTableName, 2=Alias(optional)
        const regex = /(?:FROM|JOIN)\s+([`a-zA-Z0-9_\-\.]+)(?:\s+(?:AS\s+)?([`a-zA-Z0-9_\-]+))?/gim;

        const matches = text.matchAll(regex);
        for (const m of matches) {
            const rawTable = m[1];
            const rawAlias = m[2]; // Might be undefined

            const fullPath = rawTable;
            const tableName = this.getLastNamePart(rawTable); // Extract 'table' 

            let alias = rawAlias;
            if (!alias) {
                alias = tableName; // Implicit alias is the table name
            }

            refs.push({
                tableName: this.stripQuotes(tableName),
                alias: this.stripQuotes(alias),
                fullPath: fullPath // Keep quotes for DESCRIBE
            });
        }
        return refs;
    }

    private getLastNamePart(path: string): string {
        if (path.includes('.')) {
            const parts = path.split('.');
            return parts[parts.length - 1];
        }
        return path;
    }

    private stripQuotes(str: string): string {
        if (!str) { return ''; }
        return str.replace(/`/g, '');
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
            // Uses current session context (catalog/db already set)
            const result = await this.client.listTables(sessionHandle);
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
            const result = await this.client.listTables(sessionHandle);
            this.metadataCache.set(key, result);
            return result;
        } catch (e) {
            return [];
        }
    }

    private async getColumns(sessionHandle: string, catalog: string, db: string, table: string): Promise<string[]> {
        const key = `columns:${catalog}.${db}.${table}`;
        if (this.metadataCache.has(key)) {
            return this.metadataCache.get(key)!;
        }
        try {
            const tableDetails = await this.client.getTable(sessionHandle, catalog, db, table);
            const cols = tableDetails.schema.columns.map((c: any) => c.name);
            this.metadataCache.set(key, cols);
            return cols;
        } catch (e) {
            return [];
        }
    }

    private async getColumnsGeneric(sessionHandle: string, tableRef: string): Promise<string[]> {
        const key = `columns:${tableRef}`;
        if (this.metadataCache.has(key)) {
            return this.metadataCache.get(key)!;
        }
        try {
            const rows = await this.client.runQuery(sessionHandle, `DESCRIBE ${tableRef}`);
            const cols = rows.map((r: any) => {
                if (r.fields && Array.isArray(r.fields)) { return String(r.fields[0]); }
                if (Array.isArray(r)) { return String(r[0]); }
                return null;
            }).filter(c => c !== null) as string[];

            this.metadataCache.set(key, cols);
            return cols;
        } catch (e) {
            return [];
        }
    }
}
