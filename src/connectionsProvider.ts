import * as vscode from 'vscode';
import { ConnectionManager, FlinkConnection } from './connectionManager';
import { Logger } from './utils/logger';

export class FlinkConnectionsProvider implements vscode.TreeDataProvider<ConnectionTreeItem>, vscode.Disposable {
    private _onDidChangeTreeData: vscode.EventEmitter<ConnectionTreeItem | undefined | null | void> = new vscode.EventEmitter<ConnectionTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<ConnectionTreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

    private connectionStatus: Map<string, { gateway: boolean; jobManager: boolean }> = new Map();

    constructor(private connectionManager: ConnectionManager) {
        // Listen for connection changes
        connectionManager.onDidChangeConnections(() => {
            this.refresh();
        });

        // Initial status check
        this.checkAllConnectionStatus();
    }

    dispose() {
        this._onDidChangeTreeData.dispose();
    }

    refresh(): void {
        this.checkAllConnectionStatus();
    }

    private async checkAllConnectionStatus(): Promise<void> {
        const connections = this.connectionManager.getAllConnections();

        for (const conn of connections) {
            let gatewayOk = false;
            let jobManagerOk = false;

            try {
                const response = await fetch(`${conn.gatewayUrl}/info`, { method: 'GET' });
                gatewayOk = response.ok;
            } catch {
                gatewayOk = false;
            }

            try {
                const response = await fetch(`${conn.jobManagerUrl}/overview`, { method: 'GET' });
                jobManagerOk = response.ok;
            } catch {
                jobManagerOk = false;
            }

            this.connectionStatus.set(conn.id, { gateway: gatewayOk, jobManager: jobManagerOk });
        }

        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: ConnectionTreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: ConnectionTreeItem): vscode.ProviderResult<ConnectionTreeItem[]> {
        if (element) {
            // Child items - show gateway and jobmanager URLs
            if (element.connection) {
                const status = this.connectionStatus.get(element.connection.id);
                const gatewayConnected = status?.gateway ?? false;
                const jobManagerConnected = status?.jobManager ?? false;

                return [
                    new ConnectionTreeItem(
                        'SQL Gateway',
                        vscode.TreeItemCollapsibleState.None,
                        undefined,
                        'detail',
                        element.connection.gatewayUrl,
                        new vscode.ThemeIcon('globe', gatewayConnected
                            ? new vscode.ThemeColor('charts.green')
                            : new vscode.ThemeColor('charts.red'))
                    ),
                    new ConnectionTreeItem(
                        'JobManager',
                        vscode.TreeItemCollapsibleState.None,
                        undefined,
                        'detail',
                        element.connection.jobManagerUrl,
                        new vscode.ThemeIcon('server', jobManagerConnected
                            ? new vscode.ThemeColor('charts.green')
                            : new vscode.ThemeColor('charts.red'))
                    )
                ];
            }
            return [];
        }

        // Root items - all connections
        const connections = this.connectionManager.getAllConnections();

        if (connections.length === 0) {
            return [
                new ConnectionTreeItem(
                    'No connections configured',
                    vscode.TreeItemCollapsibleState.None,
                    undefined,
                    'empty',
                    'Click + to add a connection'
                )
            ];
        }

        return connections.map(conn => {
            const status = this.connectionStatus.get(conn.id);
            const allConnected = status?.gateway && status?.jobManager;

            const icon = new vscode.ThemeIcon(
                'plug',
                allConnected
                    ? new vscode.ThemeColor('charts.green')
                    : new vscode.ThemeColor('charts.red')
            );

            return new ConnectionTreeItem(
                conn.name,
                vscode.TreeItemCollapsibleState.Collapsed,
                conn,
                'connection',
                allConnected ? 'online' : 'offline',
                icon
            );
        });
    }
}

export class ConnectionTreeItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly connection: FlinkConnection | undefined,
        public readonly itemType: 'connection' | 'detail' | 'empty',
        description?: string,
        icon?: vscode.ThemeIcon
    ) {
        super(label, collapsibleState);

        if (icon) {
            this.iconPath = icon;
        }

        if (description) {
            this.description = description;
        }

        this.contextValue = itemType;

        if (itemType === 'connection' && connection) {
            this.tooltip = `${connection.name}\nGateway: ${connection.gatewayUrl}\nJobManager: ${connection.jobManagerUrl}`;
        } else if (itemType === 'empty') {
            this.command = {
                command: 'flinkConnections.add',
                title: 'Add Connection',
                arguments: []
            };
        }
    }
}
