import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { Logger } from './utils/logger';

export class FlinkTaskManagersProvider implements vscode.WebviewViewProvider, vscode.Disposable {
    private _view?: vscode.WebviewView;
    private client: FlinkGatewayClient;
    private timer: NodeJS.Timeout | undefined;

    constructor(gatewayUrl: string, jobManagerUrl: string) {
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
            const tms = await this.client.getTaskManagers();
            const html = this.getHtmlForWebview(tms);
            this._view.webview.html = html;
        } catch (error) {
            Logger.error('Failed to fetch task managers:', error);
            // Optionally show error state in webview
        }
    }

    private formatBytes(bytes: number): string {
        if (!bytes) {
            return '0 B';
        }
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    private formatRatio(free: number | undefined, total: number | undefined, isBytes: boolean = true): string {
        if (free === undefined || total === undefined || total === 0) {
            return '-';
        }
        const used = total - free;
        const percent = Math.round((used / total) * 100);

        // Return simple percentage for cleaner table
        // Or "Free / Total"
        const freeStr = isBytes ? this.formatBytes(free) : free.toString();
        const totalStr = isBytes ? this.formatBytes(total) : total.toString();

        // Requested format: free / total
        // Let's add percentage for clarity: "50% (512MB / 1GB)" or similar?
        // User asked for "freeResource.cpuCores / totalResource.cpuCores"
        return `${freeStr} / ${totalStr} <br><span class="sub">${percent}% Used</span>`;
    }

    private getHtmlForWebview(tms: any[]): string {
        const uniqueIds = new Set<string>();
        const cards = tms.filter(tm => {
            if (!tm.id || uniqueIds.has(tm.id)) {
                return false;
            }
            uniqueIds.add(tm.id);
            return true;
        }).map(tm => {
            const shortId = tm.id.substring(0, 8) + '...';
            const heartbeat = tm.timeSinceLastHeartbeat
                ? new Date(tm.timeSinceLastHeartbeat).toLocaleTimeString()
                : '-';

            const slots = `${tm.freeSlots} / ${tm.slotsNumber}`;

            // Format: Free / Total (Percent)
            const fmt = (free: number, total: number, isBytes: boolean) => {
                if (total === 0 || total === undefined) {
                    return '-';
                }
                const p = Math.round(((total - free) / total) * 100);
                const f = isBytes ? this.formatBytes(free) : free;
                const t = isBytes ? this.formatBytes(total) : total;
                return `${f}/${t} (${p}%)`;
            };

            const row = (key: string, value: string) => `<tr><td class="k">${key}</td><td class="v">${value}</td></tr>`;

            return `
            <div class="tm">
                <table>
                    ${row('ID', shortId)}
                    ${row('Last HB', heartbeat)}
                    ${row('Total Slots', tm.slotsNumber)}
                    ${row('Free Slots', tm.freeSlots)}
                    ${row('CPU', fmt(tm.freeResource?.cpuCores, tm.totalResource?.cpuCores, false))}
                    ${row('Heap', fmt(tm.freeResource?.taskHeapMemory, tm.totalResource?.taskHeapMemory, true))}
                    ${row('Off-Heap', fmt(tm.freeResource?.taskOffHeapMemory, tm.totalResource?.taskOffHeapMemory, true))}
                    ${row('Managed', fmt(tm.freeResource?.managedMemory, tm.totalResource?.managedMemory, true))}
                    ${row('Network', fmt(tm.freeResource?.networkMemory, tm.totalResource?.networkMemory, true))}
                </table>
            </div>`;
        }).join('');

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
            padding: 5px; 
            background: transparent;
        }
        .tm {
            margin-bottom: 10px;
        }
        table { 
            width: 100%; 
            border-collapse: collapse; 
            border: 1px solid var(--vscode-widget-border);
        }
        td { 
            padding: 4px; 
            border: 1px solid var(--vscode-widget-border);
            vertical-align: top;
        }
        .k {
            width: 40%;
            font-weight: bold;
            background-color: var(--vscode-sideBar-background);
        }
        .v {
            text-align: right;
            font-family: monospace;
        }
        .no-data { padding: 20px; text-align: center; opacity: 0.7; }
    </style>
</head>
<body>
    ${cards.length > 0 ? cards : '<div class="no-data">No TaskManagers found.</div>'}
</body>
</html>`;
    }
}
