import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { Logger } from './utils/logger';

export class FlinkTaskManagersProvider implements vscode.WebviewViewProvider, vscode.Disposable {
    private _view?: vscode.WebviewView;
    private client: FlinkGatewayClient;
    private timer: NodeJS.Timeout | undefined;

    constructor(gatewayUrl: string, jobManagerUrl: string, private sessionManager: SessionManager) {
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
    }

    updateConnection(gatewayUrl: string, jobManagerUrl: string) {
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        this.refresh();
    }

    resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken,
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
        };

        webviewView.webview.onDidReceiveMessage(message => {
            if (message.command === 'copy') {
                vscode.env.clipboard.writeText(message.text);
                vscode.window.showInformationMessage(`Copied: ${message.text}`);
            }
        });

        this.updateContent();

        // Start polling when view is visible
        if (webviewView.visible) {
            this.startPolling();
        }

        webviewView.onDidChangeVisibility(() => {
            if (webviewView.visible) {
                this.startPolling();
                this.refresh();
            } else {
                this.stopPolling();
            }
        });

        webviewView.onDidDispose(() => {
            this.stopPolling();
            this._view = undefined;
        });
    }

    dispose() {
        this.stopPolling();
    }

    private startPolling() {
        if (this.timer) {
            clearInterval(this.timer);
        }
        this.timer = setInterval(() => {
            this.refresh();
        }, 5000);
    }

    private stopPolling() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = undefined;
        }
    }

    refresh(): void {
        this.updateContent();
    }

    private async updateContent() {
        if (!this._view) {
            return;
        }

        try {
            const overview = await this.client.getClusterOverview();
            const tms = await this.client.getTaskManagers();

            if (overview === null || tms === null) {
                this._view.webview.html = this.getErrorHtml('JobManager Offline');
                return;
            }

            const sessionHandle = this.sessionManager.getCurrentSessionHandle() || 'No Active Session';
            const html = this.getHtmlForWebview(overview, tms, sessionHandle);
            this._view.webview.html = html;
        } catch (error: any) {
            Logger.error('Failed to fetch task managers:', error);
            this._view.webview.html = this.getErrorHtml(error.message || 'Unknown Error');
        }
    }

    private getErrorHtml(message: string): string {
        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { 
            font-family: var(--vscode-editor-font-family); 
            font-size: var(--vscode-editor-font-size);
            color: var(--vscode-editor-foreground); 
            padding: 20px; 
            text-align: center;
        }
        .error { color: var(--vscode-errorForeground); margin-bottom: 10px; }
        .icon { font-size: 24px; margin-bottom: 10px; display: block; }
    </style>
</head>
<body>
    <div class="icon">⚠️</div>
    <div class="error">${message}</div>
    <div>Please check your connection settings.</div>
</body>
</html>`;
    }

    private formatBytes(bytes: number): string {
        if (bytes === undefined || bytes === null) { return '-'; }
        if (bytes === 0) { return '0 B'; }
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    private getHtmlForWebview(overview: any, tms: any[], sessionHandle: string): string {
        const flinkVersion = overview['flink-version'] || 'Unknown';
        const commitId = overview['flink-commit'] || '';
        const slotsTotal = overview['slots-total'];
        const slotsAvailable = overview['slots-available'];
        const jobsRunning = overview['jobs-running'];
        const jobsFinished = overview['jobs-finished'];
        const jobsCancelled = overview['jobs-cancelled'];
        const jobsFailed = overview['jobs-failed'];

        // Card Generator
        const card = (label: string, value: string | number, sub?: string) => `
            <div class="card">
                <div class="card-label">${label}</div>
                <div class="card-value">${value}</div>
                ${sub ? `<div class="card-sub">${sub}</div>` : ''}
            </div>
        `;

        // TM Generator
        const tmCards = tms.map(tm => {
            const shortId = tm.id.split('-')[0];
            const slots = `${tm.freeSlots} / ${tm.slotsNumber}`;

            // Resources (CPU/Mem) could be added here as progress bars if needed
            const cpu = tm.freeResource && tm.totalResource
                ? `${tm.freeResource.cpuCores} / ${tm.totalResource.cpuCores}`
                : '-';
            const heap = tm.freeResource && tm.totalResource
                ? this.formatBytes(tm.freeResource.taskHeapMemory) + ' / ' + this.formatBytes(tm.totalResource.taskHeapMemory)
                : '-';

            return `
            <div class="tm-card">
                <div class="tm-header">
                    <span class="tm-id" title="${tm.id}">${shortId}</span>
                    <span class="tm-hb">${tm.timeSinceLastHeartbeat ? new Date(tm.timeSinceLastHeartbeat).toLocaleTimeString() : '-'}</span>
                </div>
                <div class="tm-row">
                    <span class="tm-k">Slots</span>
                    <span class="tm-v">${slots}</span>
                </div>
                 <div class="tm-row">
                    <span class="tm-k">CPU</span>
                    <span class="tm-v">${cpu}</span>
                </div>
                 <div class="tm-row">
                    <span class="tm-k">Heap</span>
                    <span class="tm-v">${heap}</span>
                </div>
            </div>`;
        }).join('');

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        :root {
            --card-bg: var(--vscode-sideBar-background);
            --border: var(--vscode-widget-border);
            --fg: var(--vscode-editor-foreground);
            --sub-fg: var(--vscode-descriptionForeground);
            --accent: var(--vscode-textLink-foreground);
        }
        body { 
            font-family: var(--vscode-editor-font-family); 
            font-size: var(--vscode-editor-font-size);
            color: var(--fg); 
            padding: 10px; 
            background: transparent;
        }
        .header {
            margin-bottom: 15px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 5px;
        }
        .title { font-weight: bold; font-size: 1.1em; }
        .subtitle { color: var(--sub-fg); font-size: 0.85em; margin-left: 5px; }

        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-bottom: 15px;
        }
        .card {
            background: var(--vscode-editor-background);
            border: 1px solid var(--border);
            padding: 8px;
            border-radius: 4px;
            text-align: center;
        }
        .card-label { color: var(--sub-fg); font-size: 0.8em; margin-bottom: 2px; }
        .card-value { font-weight: bold; font-size: 1.2em; }
        .card-sub { font-size: 0.75em; color: var(--sub-fg); }

        .section-title {
            font-weight: bold;
            margin-bottom: 8px;
            font-size: 0.9em;
            text-transform: uppercase;
            color: var(--sub-fg);
        }

        .tm-list { display: flex; flex-direction: column; gap: 8px; }
        .tm-card {
            background: var(--vscode-editor-background);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 8px;
        }
        .tm-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 6px;
            font-size: 0.9em;
            font-weight: bold;
            border-bottom: 1px dashed var(--border);
            padding-bottom: 4px;
        }
        .tm-id { color: var(--accent); cursor: help; }
        .tm-hb { color: var(--sub-fg); font-weight: normal; font-size: 0.8em; }
        .tm-row {
            display: flex;
            justify-content: space-between;
            font-size: 0.85em;
            margin-bottom: 2px;
        }
        .tm-k { color: var(--sub-fg); }
        .tm-v { font-family: monospace; }
        
        .no-data { text-align: center; color: var(--sub-fg); margin-top: 20px; }
        .copy-btn {
            background: none;
            border: none;
            color: var(--fg);
            cursor: pointer;
            font-size: 1.2em;
            padding: 4px;
        }
        .copy-btn:hover { color: var(--accent); }
    </style>
    <script>
        const vscode = acquireVsCodeApi();
        function copyText(text) {
            vscode.postMessage({ command: 'copy', text: text });
        }
    </script>
</head>
<body>
    <div class="header">
        <span class="title">Flink Cluster</span>
        <span class="subtitle">v${flinkVersion} ${commitId ? `(${commitId.substring(0, 7)})` : ''}</span>
    </div>

    <div class="grid">
        ${card('Task Managers', tms.length)}
        ${card('Slots', slotsTotal)}
        ${card('Jobs Running', jobsRunning)}
        ${card('Jobs Finished', jobsFinished)}
    </div>

    <div class="card" style="margin-bottom: 15px; text-align: left; display: flex; justify-content: space-between; align-items: center;">
        <div>
            <div class="card-label">Session UID</div>
            <div class="card-value" style="font-size: 0.9em; font-family: monospace;">${sessionHandle}</div>
        </div>
        <button class="copy-btn" onclick="copyText('${sessionHandle}')" title="Copy Session UID">
            <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path fill-rule="evenodd" clip-rule="evenodd" d="M4 4l1-1h5.414L14 6.586V14l-1 1H5l-1-1V4zm9 3l-3-3H5v10h8V7z"/>
                <path fill-rule="evenodd" clip-rule="evenodd" d="M3 1L2 2v10l1 1V2h6.414l-1-1H3z"/>
            </svg>
        </button>
    </div>

    <div class="section-title">Task Managers</div>
    <div class="tm-list">
        ${tmCards.length > 0 ? tmCards : '<div class="no-data">No TaskManagers connected</div>'}
    </div>
</body>
</html>`;
    }
}
