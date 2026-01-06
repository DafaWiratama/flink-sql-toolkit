
import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { Logger } from './utils/logger';

export class FlinkCatalogProvider implements vscode.TreeDataProvider<CatalogTreeItem>, vscode.Disposable {
    private _onDidChangeTreeData: vscode.EventEmitter<CatalogTreeItem | undefined | null | void> = new vscode.EventEmitter<CatalogTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<CatalogTreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

    private client: FlinkGatewayClient;
    private sessionHandle: string | undefined;
    private queue: Promise<any> = Promise.resolve();

    private context: vscode.ExtensionContext;

    // Cache for tree items
    private cache = new Map<string, any>();

    private sessionManager: SessionManager;

    updateConnection(gatewayUrl: string, jobManagerUrl: string) {
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        // Session manager updated externally in extension.ts
        this.refresh();
    }

    constructor(context: vscode.ExtensionContext, gatewayUrl: string, jobManagerUrl: string, sessionManager: SessionManager) {
        this.context = context;
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        this.sessionManager = sessionManager;

        // Subscribe to session changes
        this.context.subscriptions.push(
            this.sessionManager.onDidChangeActiveSession(() => {
                this.refresh();
            })
        );
    }

    dispose() {
        // Close session if exists (optional cleanup)
    }

    refresh(): void {
        this.cache.clear();
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: CatalogTreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: CatalogTreeItem): Promise<CatalogTreeItem[]> {
        if (!element) {
            return this.listCatalogs();
        } else if (element.contextValue === 'catalog') {
            const catalog = element.label;
            const selectedDB = this.context.workspaceState.get<string>(`flink-explorer.db.${catalog}`) || 'default_database';

            // Return Groups: Tables, Views
            const tableGroup = new CatalogTreeItem('Tables', 'group-tables', vscode.TreeItemCollapsibleState.Expanded, 'list-unordered');
            tableGroup.grandParentName = catalog;
            tableGroup.parentName = selectedDB;
            tableGroup.id = `grp-tbl-${catalog}-${selectedDB}`;

            const viewGroup = new CatalogTreeItem('Views', 'group-views', vscode.TreeItemCollapsibleState.Expanded, 'layers');
            viewGroup.grandParentName = catalog;
            viewGroup.parentName = selectedDB;
            viewGroup.id = `grp-view-${catalog}-${selectedDB}`;

            return Promise.resolve([tableGroup, viewGroup]);

        } else if (element.contextValue === 'group-tables') {
            return this.listTablesFiltered(element.grandParentName!, element.parentName!);
        } else if (element.contextValue === 'group-views') {
            return this.listViewsOnly(element.grandParentName!, element.parentName!);
        }
        return Promise.resolve([]);
        return Promise.resolve([]);
    }

    private async getSession(): Promise<string> {
        return this.sessionManager.getActiveSessionHandle();
    }




    private async fetchWithCache<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
        if (this.cache.has(key)) {
            return this.cache.get(key) as Promise<T>;
        }
        const promise = fetcher().catch(e => {
            this.cache.delete(key); // Evict failed requests
            throw e;
        });
        this.cache.set(key, promise);
        return promise;
    }

    private async listCatalogs(): Promise<CatalogTreeItem[]> {
        return this.fetchWithCache('catalogs', async () => {
            try {
                Logger.info('[Explorer] Fetching catalogs...');
                const handle = await this.getSession();
                const catalogs = await this.client.listCatalogs(handle);

                if (!catalogs || catalogs.length === 0) {
                    return [];
                }

                return catalogs.map((name: string) => {
                    const selectedDB = this.context.workspaceState.get<string>(`flink-explorer.db.${name}`) || 'default_database';
                    const item = new CatalogTreeItem(name, 'catalog', vscode.TreeItemCollapsibleState.Expanded, 'server-environment');
                    item.description = `[${selectedDB}]`;
                    item.tooltip = `Active Database: ${selectedDB}`;
                    item.id = `cat-${name}`;
                    return item;
                });
            } catch (e: any) {
                Logger.error('[Explorer] listCatalogs failed:', e);
                vscode.window.showErrorMessage(`Explorer Error: ${e.message}`);
                return [];
            }
        });
    }

    async selectDatabase(item: CatalogTreeItem) {
        if (item.contextValue !== 'catalog') { return; }
        const catalog = item.label;

        let dbs: string[] = [];
        try {
            const dbItems = await this.listDatabases(catalog);
            dbs = dbItems.map(i => i.label);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to list databases: ${e.message}`);
            return;
        }

        const result = await vscode.window.showQuickPick(dbs, {
            placeHolder: `Select Database for ${catalog}`,
        });

        if (result) {
            await this.context.workspaceState.update(`flink-explorer.db.${catalog}`, result);
            this.refresh();
            vscode.window.showInformationMessage(`Selected database: ${result}`);
        }
    }

    private async listDatabases(catalog: string): Promise<CatalogTreeItem[]> {
        return this.fetchWithCache(`dbs-${catalog}`, async () => {
            try {
                const handle = await this.getSession();
                const dbs = await this.client.listDatabases(handle, catalog);
                return dbs.map((name: string) => {
                    const item = new CatalogTreeItem(name, 'database', vscode.TreeItemCollapsibleState.Expanded, 'database');
                    item.parentName = catalog;
                    item.id = `db-${catalog}-${name}`;
                    return item;
                });
            } catch (e: any) {
                console.warn(`List Databases failed: ${e.message}`);
                return [];
            }
        });
    }

    private async listTablesFiltered(catalog: string, database: string): Promise<CatalogTreeItem[]> {
        return this.fetchWithCache(`tables-only-${catalog}-${database}`, async () => {
            try {
                const handle = await this.getSession();

                // Fetch both tables (which includes views) and views
                const [allTables, views] = await Promise.all([
                    this.client.listTables(handle, catalog, database),
                    this.client.listViews(handle, catalog, database)
                ]);

                const viewSet = new Set(views);
                const tablesOnly = allTables.filter(t => !viewSet.has(t));

                return tablesOnly.map((name: string) => {
                    const item = new CatalogTreeItem(name, 'table', vscode.TreeItemCollapsibleState.None, 'table');
                    item.parentName = database;
                    item.grandParentName = catalog;
                    item.id = `tbl-${catalog}-${database}-${name}`;
                    item.command = {
                        command: 'flinkExplorer.selectObject',
                        title: 'View Details',
                        arguments: [catalog, database, name, 'table']
                    };
                    return item;
                });
            } catch (e: any) {
                console.warn(`[Explorer] List Tables failed: ${e.message}`);
                return [];
            }
        });
    }

    private async listViewsOnly(catalog: string, database: string): Promise<CatalogTreeItem[]> {
        return this.fetchWithCache(`views-only-${catalog}-${database}`, async () => {
            try {
                const handle = await this.getSession();
                const views = await this.client.listViews(handle, catalog, database);

                return views.map((name: string) => {
                    const item = new CatalogTreeItem(name, 'view', vscode.TreeItemCollapsibleState.None, 'eye'); // 'eye' icon for views
                    item.parentName = database;
                    item.grandParentName = catalog;
                    item.id = `view-${catalog}-${database}-${name}`;
                    item.command = {
                        command: 'flinkExplorer.selectObject',
                        title: 'View Details',
                        arguments: [catalog, database, name, 'view']
                    };
                    return item;
                });
            } catch (e: any) {
                console.warn(`[Explorer] List Views failed: ${e.message}`);
                return [];
            }
        });
    }

    private async listColumns(catalog: string, database: string, table: string): Promise<CatalogTreeItem[]> {
        return this.fetchWithCache(`cols-${catalog}-${database}-${table}`, async () => {
            try {
                const handle = await this.getSession();
                const tableDetails = await this.client.getTable(handle, catalog, database, table);
                const columns = tableDetails.schema?.columns || tableDetails.resolvedSchema?.columns || [];

                return columns.map((col: any) => {
                    const colName = col.name;
                    const colType = col.dataType || col.type;
                    const item = new CatalogTreeItem(`${colName} (${colType})`, 'column', vscode.TreeItemCollapsibleState.None, 'symbol-field');
                    item.id = `col-${catalog}-${database}-${table}-${colName}`;
                    return item;
                });
            } catch (e: any) {
                Logger.error('List Columns Error:', e);
                return [];
            }
        });
    }
}

class CatalogTreeItem extends vscode.TreeItem {
    public parentName?: string; // Generic parent Ref
    public grandParentName?: string;

    constructor(
        public readonly label: string,
        public readonly contextValue: string, // 'catalog' | 'database' | 'table' | 'column'
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        iconName: string
    ) {
        super(label, collapsibleState);
        this.iconPath = new vscode.ThemeIcon(iconName);
        // id assigned externally for stability
    }
}
