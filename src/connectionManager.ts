import * as vscode from 'vscode';
import { Logger } from './utils/logger';

export interface FlinkConnection {
    id: string;
    name: string;
    gatewayUrl: string;
    jobManagerUrl: string;
}

const STORAGE_KEY = 'flinkConnections';

export class ConnectionManager implements vscode.Disposable {
    private connections: FlinkConnection[] = [];

    private _onDidChangeConnections = new vscode.EventEmitter<void>();
    readonly onDidChangeConnections = this._onDidChangeConnections.event;

    constructor(private context: vscode.ExtensionContext) {
        this.load();
    }

    dispose() {
        this._onDidChangeConnections.dispose();
    }

    // Load connections from global state
    private load(): void {
        this.connections = this.context.globalState.get<FlinkConnection[]>(STORAGE_KEY, []);

        // Migration: If no connections exist, create one from legacy settings
        if (this.connections.length === 0) {
            this.migrateFromLegacySettings();
        }

        Logger.info(`[ConnectionManager] Loaded ${this.connections.length} connections`);
    }

    // Migrate from legacy single-connection settings
    private migrateFromLegacySettings(): void {
        const config = vscode.workspace.getConfiguration('flink');
        const gatewayUrl = config.get<string>('gatewayUrl', 'http://localhost:8083');
        const jobManagerUrl = config.get<string>('jobManagerUrl', 'http://localhost:8081');

        const defaultConnection: FlinkConnection = {
            id: this.generateId(),
            name: 'Local',
            gatewayUrl,
            jobManagerUrl
        };

        this.connections.push(defaultConnection);
        this.save();

        Logger.info('[ConnectionManager] Migrated legacy settings to connection');
    }

    // Save connections to global state
    private async save(): Promise<void> {
        await this.context.globalState.update(STORAGE_KEY, this.connections);
    }

    // Generate unique ID
    private generateId(): string {
        return `conn_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
    }

    // CRUD Operations

    getAllConnections(): FlinkConnection[] {
        return [...this.connections];
    }

    getConnection(id: string): FlinkConnection | undefined {
        return this.connections.find(c => c.id === id);
    }

    getFirstConnection(): FlinkConnection | undefined {
        return this.connections[0];
    }

    async addConnection(name: string, gatewayUrl: string, jobManagerUrl: string): Promise<FlinkConnection> {
        const connection: FlinkConnection = {
            id: this.generateId(),
            name,
            gatewayUrl,
            jobManagerUrl
        };

        this.connections.push(connection);
        await this.save();

        Logger.info(`[ConnectionManager] Added connection: ${name}`);
        this._onDidChangeConnections.fire();

        return connection;
    }

    async removeConnection(id: string): Promise<boolean> {
        const index = this.connections.findIndex(c => c.id === id);
        if (index === -1) {
            return false;
        }

        const removed = this.connections.splice(index, 1)[0];
        await this.save();

        Logger.info(`[ConnectionManager] Removed connection: ${removed.name}`);
        this._onDidChangeConnections.fire();

        return true;
    }

    async updateConnection(id: string, updates: Partial<Omit<FlinkConnection, 'id'>>): Promise<boolean> {
        const connection = this.connections.find(c => c.id === id);
        if (!connection) {
            return false;
        }

        if (updates.name !== undefined) { connection.name = updates.name; }
        if (updates.gatewayUrl !== undefined) { connection.gatewayUrl = updates.gatewayUrl; }
        if (updates.jobManagerUrl !== undefined) { connection.jobManagerUrl = updates.jobManagerUrl; }

        await this.save();
        Logger.info(`[ConnectionManager] Updated connection: ${connection.name}`);
        this._onDidChangeConnections.fire();

        return true;
    }

    // UI Helpers

    async promptAddConnection(): Promise<FlinkConnection | undefined> {
        const name = await vscode.window.showInputBox({
            title: 'Connection Name',
            prompt: 'Enter a name for this connection',
            placeHolder: 'e.g., Production, Development, Local',
            validateInput: (value) => {
                if (!value.trim()) { return 'Name is required'; }
                if (this.connections.some(c => c.name.toLowerCase() === value.toLowerCase())) {
                    return 'A connection with this name already exists';
                }
                return null;
            }
        });

        if (!name) { return undefined; }

        const gatewayUrl = await vscode.window.showInputBox({
            title: 'SQL Gateway URL',
            prompt: 'Enter the Flink SQL Gateway URL',
            placeHolder: 'http://localhost:8083',
            value: 'http://localhost:8083',
            validateInput: (value) => {
                if (!value.trim()) { return 'URL is required'; }
                try {
                    new URL(value);
                    return null;
                } catch {
                    return 'Invalid URL format';
                }
            }
        });

        if (!gatewayUrl) { return undefined; }

        const jobManagerUrl = await vscode.window.showInputBox({
            title: 'JobManager URL',
            prompt: 'Enter the Flink JobManager URL',
            placeHolder: 'http://localhost:8081',
            value: 'http://localhost:8081',
            validateInput: (value) => {
                if (!value.trim()) { return 'URL is required'; }
                try {
                    new URL(value);
                    return null;
                } catch {
                    return 'Invalid URL format';
                }
            }
        });

        if (!jobManagerUrl) { return undefined; }

        const connection = await this.addConnection(name, gatewayUrl, jobManagerUrl);
        vscode.window.showInformationMessage(`Connection "${name}" added successfully.`);
        return connection;
    }

    async promptEditConnection(id: string): Promise<boolean> {
        const connection = this.getConnection(id);
        if (!connection) { return false; }

        const name = await vscode.window.showInputBox({
            title: 'Connection Name',
            prompt: 'Enter a name for this connection',
            value: connection.name,
            validateInput: (value) => {
                if (!value.trim()) { return 'Name is required'; }
                if (value !== connection.name && this.connections.some(c => c.name.toLowerCase() === value.toLowerCase())) {
                    return 'A connection with this name already exists';
                }
                return null;
            }
        });

        if (!name) { return false; }

        const gatewayUrl = await vscode.window.showInputBox({
            title: 'SQL Gateway URL',
            prompt: 'Enter the Flink SQL Gateway URL',
            value: connection.gatewayUrl,
            validateInput: (value) => {
                if (!value.trim()) { return 'URL is required'; }
                try {
                    new URL(value);
                    return null;
                } catch {
                    return 'Invalid URL format';
                }
            }
        });

        if (!gatewayUrl) { return false; }

        const jobManagerUrl = await vscode.window.showInputBox({
            title: 'JobManager URL',
            prompt: 'Enter the Flink JobManager URL',
            value: connection.jobManagerUrl,
            validateInput: (value) => {
                if (!value.trim()) { return 'URL is required'; }
                try {
                    new URL(value);
                    return null;
                } catch {
                    return 'Invalid URL format';
                }
            }
        });

        if (!jobManagerUrl) { return false; }

        await this.updateConnection(id, { name, gatewayUrl, jobManagerUrl });
        vscode.window.showInformationMessage(`Connection "${name}" updated successfully.`);
        return true;
    }

    async promptRemoveConnection(id: string): Promise<boolean> {
        const connection = this.getConnection(id);
        if (!connection) { return false; }

        if (this.connections.length === 1) {
            vscode.window.showWarningMessage('Cannot remove the last connection. Add another connection first.');
            return false;
        }

        const confirm = await vscode.window.showWarningMessage(
            `Are you sure you want to remove connection "${connection.name}"?`,
            { modal: true },
            'Remove'
        );

        if (confirm !== 'Remove') { return false; }

        await this.removeConnection(id);
        vscode.window.showInformationMessage(`Connection "${connection.name}" removed.`);
        return true;
    }

    // Pick a connection from a list
    async pickConnection(title?: string): Promise<FlinkConnection | undefined> {
        if (this.connections.length === 0) {
            vscode.window.showWarningMessage('No connections available. Please add a connection first.');
            return undefined;
        }

        if (this.connections.length === 1) {
            return this.connections[0];
        }

        const items = this.connections.map(c => ({
            label: c.name,
            description: c.gatewayUrl,
            detail: `JobManager: ${c.jobManagerUrl}`,
            connection: c
        }));

        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: title || 'Select a connection',
            title: title || 'Select Connection'
        });

        return selected?.connection;
    }
}
