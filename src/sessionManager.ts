import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';

interface SessionInfo {
    name: string;
    handle: string;
    createdAt: number;
}

export class SessionManager implements vscode.Disposable {
    private client: FlinkGatewayClient;
    private statusBarItem: vscode.StatusBarItem;
    private _sessions: SessionInfo[] = [];
    private _activeSessionHandle: string | undefined;

    private _onDidChangeActiveSession = new vscode.EventEmitter<string>();
    readonly onDidChangeActiveSession = this._onDidChangeActiveSession.event;

    constructor(private context: vscode.ExtensionContext, client: FlinkGatewayClient) {
        this.client = client;
        this.statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
        this.statusBarItem.command = 'flink.selectSession';
        this.context.subscriptions.push(this.statusBarItem);

        // Load sessions from storage
        this._loadSessions();

        // Initial update
        this.updateStatusBar();
        this.statusBarItem.show();
    }

    dispose() {
        this.statusBarItem.dispose();
        this._onDidChangeActiveSession.dispose();
    }

    updateClient(client: FlinkGatewayClient) {
        this.client = client;
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

    private updateStatusBar() {
        if (this._activeSessionHandle) {
            const session = this._sessions.find(s => s.handle === this._activeSessionHandle);
            this.statusBarItem.text = `$(pulse) Flink: ${session ? session.name : 'Unknown Session'}`;
            this.statusBarItem.tooltip = `Active Session Handle: ${this._activeSessionHandle}`;
        } else {
            this.statusBarItem.text = '$(pulse) Flink: No Session';
            this.statusBarItem.tooltip = 'Click to create or select a session';
        }
    }

    async getActiveSessionHandle(): Promise<string> {
        if (!this._activeSessionHandle) {
            // Auto-create if none exists
            if (this._sessions.length === 0) {
                return await this.createSession('default_session');
            }

            // Or prompt user
            const selected = await this.pickSession();
            if (!selected) {
                throw new Error('No active Flink session selected.');
            }
            return selected;
        }

        // Validate session is still alive
        const isValid = await this.client.checkSession(this._activeSessionHandle);
        if (!isValid) {
            const selection = await vscode.window.showWarningMessage(
                `Session ${this._activeSessionHandle} is no longer valid.`,
                'Create New', 'Select Another'
            );

            this._removeSession(this._activeSessionHandle);

            if (selection === 'Create New') {
                return await this.createSession();
            } else if (selection === 'Select Another') {
                const newHandle = await this.pickSession();
                if (!newHandle) { throw new Error('No session selected'); }
                return newHandle;
            } else {
                throw new Error('Session invalid');
            }
        }

        return this._activeSessionHandle;
    }

    async createSession(name?: string): Promise<string> {
        if (!name) {
            name = await vscode.window.showInputBox({
                prompt: 'Enter session name',
                placeHolder: 'my-session',
                value: `session_${Date.now()}`
            });
        }

        if (!name) { return ''; } // Cancelled

        try {
            const { sessionHandle } = await this.client.createSession(name);
            const newSession: SessionInfo = {
                name,
                handle: sessionHandle,
                createdAt: Date.now()
            };

            this._sessions.push(newSession);
            await this._setAsActive(sessionHandle);
            await this._saveSessions();

            vscode.window.showInformationMessage(`Created session '${name}'`);
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
            ...this._sessions.map(s => ({
                label: s.handle === this._activeSessionHandle ? `$(check) ${s.name}` : s.name,
                description: s.handle,
                detail: `Created: ${new Date(s.createdAt).toLocaleTimeString()}`
            }))
        ];

        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: 'Select Flink Session'
        });

        if (!selected) { return undefined; }

        if (selected.label.includes('Create New Session')) {
            return this.createSession();
        }

        // It's a session item
        const handle = selected.description;
        if (handle) {
            await this._setAsActive(handle);
            return handle;
        }
        return undefined;
    }

    private async _setAsActive(handle: string) {
        this._activeSessionHandle = handle;
        this.updateStatusBar();
        await this._saveSessions();
        this._onDidChangeActiveSession.fire(handle);
    }

    private _removeSession(handle: string) {
        this._sessions = this._sessions.filter(s => s.handle !== handle);
        if (this._activeSessionHandle === handle) {
            this._activeSessionHandle = undefined;
            this.updateStatusBar();
        }
        this._saveSessions();
    }
}
