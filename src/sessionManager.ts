import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { ConnectionManager, FlinkConnection } from './connectionManager';
import { Logger } from './utils/logger';

export interface SessionInfo {
    name: string;
    handle: string;
    connectionId: string;  // The connection this session uses
    createdAt: number;
}

export class SessionManager implements vscode.Disposable {
    private _sessions: SessionInfo[] = [];
    private _activeSessionHandle: string | undefined;

    private _onDidChangeActiveSession = new vscode.EventEmitter<string>();
    readonly onDidChangeActiveSession = this._onDidChangeActiveSession.event;

    private _onDidChangeSessions = new vscode.EventEmitter<void>();
    readonly onDidChangeSessions = this._onDidChangeSessions.event;

    constructor(
        private context: vscode.ExtensionContext,
        private connectionManager: ConnectionManager
    ) {
        // Load sessions from storage
        this._loadSessions();
    }

    dispose() {
        this._onDidChangeActiveSession.dispose();
        this._onDidChangeSessions.dispose();
    }

    private _loadSessions() {
        const saved = this.context.workspaceState.get<SessionInfo[]>('flinkSessionsList', []);
        const activeHandle = this.context.workspaceState.get<string>('flinkActiveSessionHandle');

        this._sessions = saved;
        this._activeSessionHandle = activeHandle;

        // If we have sessions but no active one, default to the most recent
        if (this._sessions.length > 0 && !this._activeSessionHandle) {
            this._activeSessionHandle = this._sessions[0].handle;
        }
    }

    private async _saveSessions() {
        await this.context.workspaceState.update('flinkSessionsList', this._sessions);
        await this.context.workspaceState.update('flinkActiveSessionHandle', this._activeSessionHandle);
    }

    // Get client for a specific session
    getClientForSession(sessionHandle: string): FlinkGatewayClient | undefined {
        const session = this._sessions.find(s => s.handle === sessionHandle);
        if (!session) { return undefined; }

        const conn = this.connectionManager.getConnection(session.connectionId);
        if (!conn) { return undefined; }

        return new FlinkGatewayClient(conn.gatewayUrl, conn.jobManagerUrl);
    }

    // Get connection for a session
    getConnectionForSession(sessionHandle: string): FlinkConnection | undefined {
        const session = this._sessions.find(s => s.handle === sessionHandle);
        if (!session) { return undefined; }
        return this.connectionManager.getConnection(session.connectionId);
    }

    // Get all sessions
    getAllSessions(): SessionInfo[] {
        return [...this._sessions];
    }

    // Get session by handle
    getSession(handle: string): SessionInfo | undefined {
        return this._sessions.find(s => s.handle === handle);
    }

    async getActiveSessionHandle(): Promise<string> {
        if (!this._activeSessionHandle) {
            // Auto-create if none exists
            if (this._sessions.length === 0) {
                return await this.createSession();
            }

            // Or prompt user
            const selected = await this.pickSession();
            if (!selected) {
                throw new Error('No active Flink session selected.');
            }
            return selected;
        }

        // Validate session is still alive
        const client = this.getClientForSession(this._activeSessionHandle);
        if (!client) {
            this._removeSession(this._activeSessionHandle);
            throw new Error('Session connection no longer exists.');
        }

        const isValid = await client.checkSession(this._activeSessionHandle);
        if (!isValid) {
            Logger.info(`[SessionManager] Session ${this._activeSessionHandle} is invalid. Auto-recovering...`);

            // Get connection info before removing
            const session = this.getSession(this._activeSessionHandle);
            const connectionId = session?.connectionId;

            this._removeSession(this._activeSessionHandle);

            // Auto-create 'default' session on the same connection
            return await this.createSession('default', connectionId);
        }

        return this._activeSessionHandle;
    }

    getCurrentSessionHandle(): string | undefined {
        return this._activeSessionHandle;
    }

    async createSession(name?: string, connectionId?: string): Promise<string> {
        // First, pick a connection if not provided
        let connection: FlinkConnection | undefined;
        if (connectionId) {
            connection = this.connectionManager.getConnection(connectionId);
        }

        if (!connection) {
            connection = await this.connectionManager.pickConnection('Select connection for new session');
            if (!connection) { return ''; } // Cancelled
        }

        // Then get session name
        if (!name) {
            name = await vscode.window.showInputBox({
                prompt: 'Enter session name',
                placeHolder: 'my-session',
                value: `session_${Date.now()}`
            });
        }

        if (!name) { return ''; } // Cancelled

        try {
            const client = new FlinkGatewayClient(connection.gatewayUrl, connection.jobManagerUrl);
            const { sessionHandle } = await client.createSession(name);

            const newSession: SessionInfo = {
                name,
                handle: sessionHandle,
                connectionId: connection.id,
                createdAt: Date.now()
            };

            this._sessions.push(newSession);
            await this._setAsActive(sessionHandle);
            await this._saveSessions();

            this._onDidChangeSessions.fire();

            vscode.window.showInformationMessage(`Created session '${name}' on ${connection.name}`);
            return sessionHandle;
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to create session: ${e.message}`);
            throw e;
        }
    }

    async pickSession(): Promise<string | undefined> {
        const items: vscode.QuickPickItem[] = [
            { label: '$(add) Create New Session', description: 'Create a new Flink session' },
            { label: '', kind: vscode.QuickPickItemKind.Separator },
            ...this._sessions.map(s => {
                const conn = this.connectionManager.getConnection(s.connectionId);
                return {
                    label: s.handle === this._activeSessionHandle ? `$(check) ${s.name}` : s.name,
                    description: `@ ${conn?.name || 'Unknown'}`,
                    detail: `Created: ${new Date(s.createdAt).toLocaleTimeString()}`,
                    handle: s.handle
                };
            })
        ];

        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: 'Select Flink Session'
        });

        if (!selected) { return undefined; }

        if (selected.label.includes('Create New Session')) {
            return this.createSession();
        }

        // It's a session item
        const handle = (selected as any).handle;
        if (handle) {
            await this._setAsActive(handle);
            return handle;
        }
        return undefined;
    }

    async setActiveSession(handle: string): Promise<boolean> {
        const session = this._sessions.find(s => s.handle === handle);
        if (!session) {
            return false;
        }
        await this._setAsActive(handle);
        return true;
    }

    private async _setAsActive(handle: string) {
        this._activeSessionHandle = handle;
        await this._saveSessions();
        this._onDidChangeActiveSession.fire(handle);
    }

    async removeSession(handle: string): Promise<boolean> {
        if (this._sessions.length === 1) {
            vscode.window.showWarningMessage('Cannot remove the last session.');
            return false;
        }

        const session = this._sessions.find(s => s.handle === handle);
        if (!session) {
            return false;
        }

        const confirm = await vscode.window.showWarningMessage(
            `Are you sure you want to remove session "${session.name}"?`,
            { modal: true },
            'Remove'
        );

        if (confirm !== 'Remove') {
            return false;
        }

        this._removeSession(handle);
        vscode.window.showInformationMessage(`Session "${session.name}" removed.`);
        return true;
    }

    private _removeSession(handle: string) {
        this._sessions = this._sessions.filter(s => s.handle !== handle);
        if (this._activeSessionHandle === handle) {
            this._activeSessionHandle = this._sessions.length > 0 ? this._sessions[0].handle : undefined;
        }
        this._saveSessions();
        this._onDidChangeSessions.fire();
    }

    async validateOrRecoverSession(handle: string): Promise<string> {
        const client = this.getClientForSession(handle);
        if (!client) {
            // Cannot validate without client, but maybe session info exists
            const session = this.getSession(handle);
            if (session) {
                // Remove invalid session
                this._removeSession(handle);
                // Attempt recovery if we have connectionId
                Logger.info(`[SessionManager] Session ${handle} client missing. Auto-recovering...`);
                return await this.createSession('default', session.connectionId);
            }
            // No session info? Return empty or throw
            throw new Error('Session connection no longer exists.');
        }

        const isValid = await client.checkSession(handle);
        if (!isValid) {
            Logger.info(`[SessionManager] Session ${handle} is invalid. Auto-recovering...`);

            // Get connection info before removing
            const session = this.getSession(handle);
            const connectionId = session?.connectionId;

            this._removeSession(handle);

            // Auto-create 'default' session on the same connection
            return await this.createSession('default', connectionId);
        }

        return handle;
    }
}
