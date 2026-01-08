import * as vscode from 'vscode';
import { SessionManager, SessionInfo } from './sessionManager';
import { ConnectionManager } from './connectionManager';
import { Logger } from './utils/logger';

export class FlinkSessionsProvider implements vscode.TreeDataProvider<SessionTreeItem>, vscode.Disposable {
    private _onDidChangeTreeData: vscode.EventEmitter<SessionTreeItem | undefined | null | void> = new vscode.EventEmitter<SessionTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<SessionTreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor(
        private sessionManager: SessionManager,
        private connectionManager: ConnectionManager
    ) {
        // Listen for session changes
        sessionManager.onDidChangeSessions(() => {
            this._onDidChangeTreeData.fire();
        });

        sessionManager.onDidChangeActiveSession(() => {
            this._onDidChangeTreeData.fire();
        });
    }

    dispose() {
        this._onDidChangeTreeData.dispose();
    }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: SessionTreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: SessionTreeItem): vscode.ProviderResult<SessionTreeItem[]> {
        if (element) {
            // No children for sessions
            return [];
        }

        // Root items - all sessions
        const sessions = this.sessionManager.getAllSessions();
        const activeHandle = this.sessionManager.getCurrentSessionHandle();

        if (sessions.length === 0) {
            return [
                new SessionTreeItem(
                    'No sessions',
                    vscode.TreeItemCollapsibleState.None,
                    undefined,
                    'empty',
                    'Click + to create a session'
                )
            ];
        }

        return sessions.map(session => {
            const isActive = session.handle === activeHandle;
            const conn = this.connectionManager.getConnection(session.connectionId);

            const icon = new vscode.ThemeIcon(
                isActive ? 'terminal-tmux' : 'circle-outline',
                isActive
                    ? new vscode.ThemeColor('charts.green')
                    : new vscode.ThemeColor('descriptionForeground')
            );

            return new SessionTreeItem(
                session.name,
                vscode.TreeItemCollapsibleState.None,
                session,
                'session',
                isActive ? `● ${conn?.name || 'Unknown'}` : conn?.name || 'Unknown',
                icon
            );
        });
    }
}

export class SessionTreeItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly session: SessionInfo | undefined,
        public readonly itemType: 'session' | 'detail' | 'empty',
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

        if (itemType === 'session' && session) {
            this.tooltip = `Session: ${session.name}\nHandle: ${session.handle}\nCreated: ${new Date(session.createdAt).toLocaleString()}\n\nClick to set as active for Explorer`;

            // Click to set as active session (used by Explorer)
            this.command = {
                command: 'flinkSessions.setActive',
                title: 'Set as Active',
                arguments: [this]
            };
        } else if (itemType === 'empty') {
            this.command = {
                command: 'flinkSessions.create',
                title: 'Create Session',
                arguments: []
            };
        }
    }
}
