import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';

export class FlinkCatalogProvider implements vscode.TreeDataProvider<CatalogTreeItem>, vscode.Disposable {
    private _onDidChangeTreeData = new vscode.EventEmitter<CatalogTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private client: FlinkGatewayClient;
    private context: vscode.ExtensionContext;
    private cache = new Map<string, Promise<any>>();
    private sessionManager: SessionManager;

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

    dispose() { }

    refresh(): void {
        this.cache.clear();
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: CatalogTreeItem): vscode.TreeItem {
        return element;
    }

    async getChildren(element?: CatalogTreeItem): Promise<CatalogTreeItem[]> {
        if (!element) {
            return this.getRoot();
        }

        if (element.contextValue === 'root-catalog') {
            return this.getCatalogChildren(element.label, element.currentDatabase || 'default_database');
        }

        if (element.contextValue === 'group-tables') {
            return this.getObjects('TABLE', element.grandParentName!, element.parentName!);
        }

        if (element.contextValue === 'group-views') {
            return this.getObjects('VIEW', element.grandParentName!, element.parentName!);
        }

        return [];
    }

    // --- Tree Building ---

    private async getRoot(): Promise<CatalogTreeItem[]> {
        return this.cached('root', async () => {
            try {
                // Get current catalog and database from session
                const handle = await this.sessionManager.getActiveSessionHandle();
                const currentCatalog = await this.fetchCurrentCatalog(handle);
                const currentDatabase = await this.fetchCurrentDatabase(handle);

                if (!currentCatalog) {
                    return [];
                }

                const item = new CatalogTreeItem(
                    currentCatalog,
                    'root-catalog',
                    vscode.TreeItemCollapsibleState.Expanded,
                    'server-environment'
                );
                item.description = `[${currentDatabase}]`;
                item.tooltip = `Current Catalog: ${currentCatalog}\nCurrent Database: ${currentDatabase}`;
                item.id = `cat-${currentCatalog}-${currentDatabase}`;
                item.currentDatabase = currentDatabase;
                return [item];
            } catch (e: any) {
                vscode.window.showErrorMessage(`Explorer Error: ${e.message}`);
                return [];
            }
        });
    }

    private getCatalogChildren(catalog: string, database: string): CatalogTreeItem[] {
        const tableGroup = new CatalogTreeItem('Tables', 'group-tables', vscode.TreeItemCollapsibleState.Expanded, 'list-unordered');
        tableGroup.grandParentName = catalog;
        tableGroup.parentName = database;
        tableGroup.id = `grp-tbl-${catalog}-${database}`;

        const viewGroup = new CatalogTreeItem('Views', 'group-views', vscode.TreeItemCollapsibleState.Expanded, 'layers');
        viewGroup.grandParentName = catalog;
        viewGroup.parentName = database;
        viewGroup.id = `grp-view-${catalog}-${database}`;

        return [tableGroup, viewGroup];
    }

    private async getObjects(kind: 'TABLE' | 'VIEW', catalog: string, database: string): Promise<CatalogTreeItem[]> {
        const allObjects = await this.fetchObjectsWithKind();
        const filtered = allObjects.filter(o => o.kind === kind);
        const iconName = kind === 'TABLE' ? 'table' : 'eye';
        const contextValue = kind === 'TABLE' ? 'table' : 'view';

        return filtered.map(obj => {
            const item = new CatalogTreeItem(obj.name, contextValue, vscode.TreeItemCollapsibleState.None, iconName);
            item.parentName = database;
            item.grandParentName = catalog;
            item.id = `${kind.toLowerCase()}-${catalog}-${database}-${obj.name}`;
            item.command = {
                command: 'flinkExplorer.selectObject',
                title: 'View Details',
                arguments: [catalog, database, obj.name, kind.toLowerCase()]
            };
            return item;
        });
    }

    // --- User Actions ---

    async selectCatalog() {
        const catalogs = await this.fetchCatalogs();
        if (catalogs.length === 0) {
            return;
        }

        const result = await vscode.window.showQuickPick(catalogs, {
            placeHolder: 'Select Catalog to Explore'
        });

        if (result) {
            try {
                // Update session context using USE CATALOG
                const handle = await this.sessionManager.getActiveSessionHandle();
                await this.client.useCatalog(handle, result);
                this.refresh();
                vscode.window.showInformationMessage(`Switched to catalog: ${result}`);
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to switch catalog: ${e.message}`);
            }
        }
    }

    async selectDatabase(item?: CatalogTreeItem) {
        try {
            const handle = await this.sessionManager.getActiveSessionHandle();
            const currentCatalog = await this.fetchCurrentCatalog(handle);

            if (!currentCatalog) {
                vscode.window.showWarningMessage('No catalog selected');
                return;
            }

            const databases = await this.fetchDatabases(currentCatalog);
            if (databases.length === 0) {
                return;
            }

            const result = await vscode.window.showQuickPick(databases, {
                placeHolder: `Select Database for ${currentCatalog}`
            });

            if (result) {
                // Update session context using USE DATABASE
                await this.client.useDatabase(handle, result);
                this.refresh();
                vscode.window.showInformationMessage(`Switched to database: ${currentCatalog}.${result}`);
            }
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to switch database: ${e.message}`);
        }
    }

    // --- Data Fetching ---

    private async fetchCurrentCatalog(handle: string): Promise<string> {
        try {
            const rows = await this.client.runQuery(handle, 'SHOW CURRENT CATALOG');
            if (rows.length > 0 && rows[0].fields && rows[0].fields.length > 0) {
                return rows[0].fields[0];
            }
            return 'default_catalog';
        } catch {
            return 'default_catalog';
        }
    }

    private async fetchCurrentDatabase(handle: string): Promise<string> {
        try {
            const rows = await this.client.runQuery(handle, 'SHOW CURRENT DATABASE');
            if (rows.length > 0 && rows[0].fields && rows[0].fields.length > 0) {
                return rows[0].fields[0];
            }
            return 'default_database';
        } catch {
            return 'default_database';
        }
    }

    private async fetchCatalogs(): Promise<string[]> {
        try {
            const handle = await this.sessionManager.getActiveSessionHandle();
            return await this.client.listCatalogs(handle);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Explorer Error: ${e.message}`);
            return [];
        }
    }

    private async fetchDatabases(catalog: string): Promise<string[]> {
        return this.cached(`dbs-${catalog}`, async () => {
            try {
                const handle = await this.sessionManager.getActiveSessionHandle();
                return await this.client.listDatabases(handle, catalog);
            } catch {
                return [];
            }
        });
    }

    private async fetchObjectsWithKind(): Promise<{ name: string, kind: string }[]> {
        return this.cached('objects', async () => {
            try {
                const handle = await this.sessionManager.getActiveSessionHandle();
                return await this.client.listTablesWithKind(handle);
            } catch {
                return [];
            }
        });
    }

    // --- Helpers ---

    private async cached<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
        if (!this.cache.has(key)) {
            const promise = fetcher().catch(e => {
                this.cache.delete(key);
                throw e;
            });
            this.cache.set(key, promise);
        }
        return this.cache.get(key) as Promise<T>;
    }
}

class CatalogTreeItem extends vscode.TreeItem {
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
