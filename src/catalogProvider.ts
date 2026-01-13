import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { Logger } from './utils/logger';

/**
 * Optimized Flink Catalog Provider with:
 * - Lazy loading: Data is only fetched when nodes are expanded
 * - TTL-based caching: Cache entries expire after a configurable time
 * - Separate caches for tables/views: Avoids redundant fetches
 * - Batched object discovery: Single query for both tables and views per database
 */
export class FlinkCatalogProvider implements vscode.TreeDataProvider<CatalogTreeItem>, vscode.Disposable {
    private _onDidChangeTreeData = new vscode.EventEmitter<CatalogTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private client: FlinkGatewayClient;
    private context: vscode.ExtensionContext;
    private sessionManager: SessionManager;

    // TTL-based cache with timestamps
    private cache = new Map<string, { data: any; timestamp: number }>();
    private readonly CACHE_TTL_MS = 60000; // 1 minute TTL

    // Pending requests to avoid duplicate concurrent fetches
    private pendingRequests = new Map<string, Promise<any>>();

    constructor(context: vscode.ExtensionContext, gatewayUrl: string, jobManagerUrl: string, sessionManager: SessionManager) {
        this.context = context;
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        this.sessionManager = sessionManager;

        // Refresh on session changes
        this.context.subscriptions.push(
            this.sessionManager.onDidChangeActiveSession(() => this.refresh())
        );
    }

    updateConnection(gatewayUrl: string, jobManagerUrl: string) {
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        this.refresh();
    }

    dispose() {
        this._onDidChangeTreeData.dispose();
    }

    refresh(): void {
        this.cache.clear();
        this.pendingRequests.clear();
        this._onDidChangeTreeData.fire();
    }

    /**
     * Refresh a specific node and its children (partial refresh)
     */
    refreshNode(element?: CatalogTreeItem): void {
        if (element) {
            // Clear cache for this node's children only
            const prefix = this.getCachePrefix(element);
            for (const key of this.cache.keys()) {
                if (key.startsWith(prefix)) {
                    this.cache.delete(key);
                }
            }
        }
        this._onDidChangeTreeData.fire(element);
    }

    private getCachePrefix(element: CatalogTreeItem): string {
        if (element.contextValue === 'root-catalog') {
            return `dbs-${element.label}`;
        }
        if (element.contextValue === 'database') {
            return `obj-${element.grandParentName}-${element.label}`;
        }
        return '';
    }

    getTreeItem(element: CatalogTreeItem): vscode.TreeItem {
        return element;
    }

    async getChildren(element?: CatalogTreeItem): Promise<CatalogTreeItem[]> {
        // Root level - show catalogs
        if (!element) {
            return this.getCatalogs();
        }

        // Catalog node -> show all databases (lazy loaded)
        if (element.contextValue === 'root-catalog') {
            return this.getDatabases(element.label);
        }

        // Database node -> fetch tables and views in ONE step, show groups with counts
        if (element.contextValue === 'database') {
            return this.getDatabaseChildren(element.grandParentName!, element.label);
        }

        // Tables group -> return pre-fetched tables
        if (element.contextValue === 'group-tables') {
            return this.getTablesFromCache(element.grandParentName!, element.parentName!);
        }

        // Views group -> return pre-fetched views
        if (element.contextValue === 'group-views') {
            return this.getViewsFromCache(element.grandParentName!, element.parentName!);
        }

        return [];
    }

    // --- Tree Building (Lazy Loaded) ---

    private async getCatalogs(): Promise<CatalogTreeItem[]> {
        return this.cachedFetch('catalogs', async () => {
            const handle = await this.sessionManager.getActiveSessionHandle();
            const catalogs = await this.client.listCatalogs(handle);

            return catalogs.map(catalog => {
                const item = new CatalogTreeItem(
                    catalog,
                    'root-catalog',
                    vscode.TreeItemCollapsibleState.Collapsed,
                    'server-environment'
                );
                item.tooltip = `Catalog: ${catalog}`;
                item.id = `cat-${catalog}`;
                return item;
            });
        });
    }

    private async getDatabases(catalog: string): Promise<CatalogTreeItem[]> {
        return this.cachedFetch(`dbs-${catalog}`, async () => {
            const handle = await this.sessionManager.getActiveSessionHandle();
            const databases = await this.client.listDatabases(handle, catalog);

            return databases.map(db => {
                const item = new CatalogTreeItem(
                    db,
                    'database',
                    vscode.TreeItemCollapsibleState.Collapsed,
                    'database'
                );
                item.grandParentName = catalog;
                item.id = `db-${catalog}-${db}`;
                item.tooltip = `Database: ${catalog}.${db}`;
                return item;
            });
        });
    }

    /**
     * Fetch all tables and views in ONE step when database is expanded
     * Returns Tables and Views groups with counts
     */
    private async getDatabaseChildren(catalog: string, database: string): Promise<CatalogTreeItem[]> {
        // Fetch all objects in one call
        const handle = await this.sessionManager.getActiveSessionHandle();
        const objects = await this.fetchObjectsForDatabase(handle, catalog, database);

        const tables = objects.filter(o => o.kind === 'TABLE');
        const views = objects.filter(o => o.kind === 'VIEW');

        const tableGroup = new CatalogTreeItem(
            'Tables',
            'group-tables',
            tables.length > 0 ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None,
            'list-unordered'
        );
        tableGroup.grandParentName = catalog;
        tableGroup.parentName = database;
        tableGroup.id = `grp-tbl-${catalog}-${database}`;
        tableGroup.description = `(${tables.length})`;

        const viewGroup = new CatalogTreeItem(
            'Views',
            'group-views',
            views.length > 0 ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None,
            'layers'
        );
        viewGroup.grandParentName = catalog;
        viewGroup.parentName = database;
        viewGroup.id = `grp-view-${catalog}-${database}`;
        viewGroup.description = `(${views.length})`;

        return [tableGroup, viewGroup];
    }

    /**
     * Return tables from cache (already fetched when database was expanded)
     */
    private async getTablesFromCache(catalog: string, database: string): Promise<CatalogTreeItem[]> {
        const cacheKey = `obj-${catalog}-${database}`;
        const cached = this.cache.get(cacheKey);

        if (cached) {
            const objects = cached.data as { name: string; kind: string }[];
            return objects
                .filter(o => o.kind === 'TABLE')
                .map(obj => this.createObjectItem(obj.name, 'table', catalog, database));
        }

        // Fallback: fetch if not cached (shouldn't happen normally)
        const handle = await this.sessionManager.getActiveSessionHandle();
        const objects = await this.fetchObjectsForDatabase(handle, catalog, database);
        return objects
            .filter(o => o.kind === 'TABLE')
            .map(obj => this.createObjectItem(obj.name, 'table', catalog, database));
    }

    /**
     * Return views from cache (already fetched when database was expanded)
     */
    private async getViewsFromCache(catalog: string, database: string): Promise<CatalogTreeItem[]> {
        const cacheKey = `obj-${catalog}-${database}`;
        const cached = this.cache.get(cacheKey);

        if (cached) {
            const objects = cached.data as { name: string; kind: string }[];
            return objects
                .filter(o => o.kind === 'VIEW')
                .map(obj => this.createObjectItem(obj.name, 'view', catalog, database));
        }

        // Fallback: fetch if not cached (shouldn't happen normally)
        const handle = await this.sessionManager.getActiveSessionHandle();
        const objects = await this.fetchObjectsForDatabase(handle, catalog, database);
        return objects
            .filter(o => o.kind === 'VIEW')
            .map(obj => this.createObjectItem(obj.name, 'view', catalog, database));
    }

    /**
     * Batched fetch for all objects in a database (tables + views)
     */
    private async fetchObjectsForDatabase(handle: string, catalog: string, database: string): Promise<{ name: string; kind: string }[]> {
        const cacheKey = `obj-${catalog}-${database}`;

        return this.cachedFetch(cacheKey, async () => {
            Logger.info(`[Explorer] Fetching objects for ${catalog}.${database}`);
            return await this.client.listTablesWithKindInDatabase(handle, catalog, database);
        });
    }

    /**
     * Refresh a specific database - clears cache and refreshes its children
     */
    refreshDatabase(item: CatalogTreeItem) {
        if (item.contextValue !== 'database' || !item.grandParentName) {
            return;
        }

        const catalog = item.grandParentName;
        const database = item.label;

        // Clear cache for this database's objects
        this.cache.delete(`obj-${catalog}-${database}`);
        this.pendingRequests.delete(`obj-${catalog}-${database}`);

        // Refresh this node
        this._onDidChangeTreeData.fire(item);
        Logger.info(`[Explorer] Refreshed ${catalog}.${database}`);
    }

    /**
     * Refresh a specific catalog - clears cache and refreshes its database list
     */
    refreshCatalog(item: CatalogTreeItem) {
        if (item.contextValue !== 'root-catalog') {
            return;
        }

        const catalog = item.label;

        // Clear cache for this catalog's databases
        this.cache.delete(`dbs-${catalog}`);
        this.pendingRequests.delete(`dbs-${catalog}`);

        // Refresh this node
        this._onDidChangeTreeData.fire(item);
        Logger.info(`[Explorer] Refreshed catalog ${catalog}`);
    }

    private createObjectItem(name: string, type: 'table' | 'view', catalog: string, database: string): CatalogTreeItem {
        const iconName = type === 'table' ? 'table' : 'eye';
        const item = new CatalogTreeItem(name, type, vscode.TreeItemCollapsibleState.None, iconName);
        item.parentName = database;
        item.grandParentName = catalog;
        item.id = `${type}-${catalog}-${database}-${name}`;
        item.command = {
            command: 'flinkExplorer.selectObject',
            title: 'View Details',
            arguments: [catalog, database, name, type]
        };
        return item;
    }

    // --- User Actions ---

    async selectCatalog() {
        try {
            const handle = await this.sessionManager.getActiveSessionHandle();
            const catalogs = await this.client.listCatalogs(handle);

            if (catalogs.length === 0) {
                vscode.window.showWarningMessage('No catalogs available');
                return;
            }

            const result = await vscode.window.showQuickPick(catalogs, {
                placeHolder: 'Select Catalog to Use'
            });

            if (result) {
                await this.client.useCatalog(handle, result);
                vscode.window.showInformationMessage(`Switched to catalog: ${result}`);
            }
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to switch catalog: ${e.message}`);
        }
    }

    async selectDatabase(item?: CatalogTreeItem) {
        if (!item || item.contextValue !== 'database' || !item.grandParentName) {
            vscode.window.showWarningMessage('Please select a database from the Explorer tree');
            return;
        }

        try {
            const handle = await this.sessionManager.getActiveSessionHandle();
            await this.client.useCatalog(handle, item.grandParentName);
            await this.client.useDatabase(handle, item.label);
            vscode.window.showInformationMessage(`Switched to database: ${item.grandParentName}.${item.label}`);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to switch database: ${e.message}`);
        }
    }

    // --- Caching with TTL and Request Deduplication ---

    /**
     * Fetch with caching, TTL, and request deduplication
     * - Returns cached data if valid (within TTL)
     * - Deduplicates concurrent requests for the same key
     * - Automatically removes failed requests from cache
     */
    private async cachedFetch<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
        // Check if we have valid cached data
        const cached = this.cache.get(key);
        if (cached && (Date.now() - cached.timestamp) < this.CACHE_TTL_MS) {
            return cached.data as T;
        }

        // Check if there's already a pending request for this key
        if (this.pendingRequests.has(key)) {
            return this.pendingRequests.get(key) as Promise<T>;
        }

        // Create new request
        const request = fetcher()
            .then(data => {
                this.cache.set(key, { data, timestamp: Date.now() });
                this.pendingRequests.delete(key);
                return data;
            })
            .catch(e => {
                this.pendingRequests.delete(key);
                this.cache.delete(key);
                Logger.warn(`[Explorer] Failed to fetch ${key}: ${e.message}`);
                throw e;
            });

        this.pendingRequests.set(key, request);
        return request;
    }
}

export class CatalogTreeItem extends vscode.TreeItem {
    public parentName?: string;
    public grandParentName?: string;
    public currentDatabase?: string;

    constructor(
        public readonly label: string,
        public readonly contextValue: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        iconName: string
    ) {
        super(label, collapsibleState);
        this.iconPath = new vscode.ThemeIcon(iconName);
    }
}
