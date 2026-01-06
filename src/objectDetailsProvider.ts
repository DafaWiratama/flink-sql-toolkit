
import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { Logger } from './utils/logger';

export class FlinkObjectDetailsProvider implements vscode.WebviewViewProvider {
    private _view?: vscode.WebviewView;
    private client: FlinkGatewayClient;
    private sessionManager: SessionManager;

    constructor(client: FlinkGatewayClient, sessionManager: SessionManager) {
        this.client = client;
        this.sessionManager = sessionManager;
    }

    private pendingUpdate: { catalog: string; database: string; object: string; type: 'table' | 'view' } | undefined;

    resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken,
    ) {
        this._view = webviewView;
        webviewView.webview.options = {
            enableScripts: true,
        };

        if (this.pendingUpdate) {
            this.update(this.pendingUpdate.catalog, this.pendingUpdate.database, this.pendingUpdate.object, this.pendingUpdate.type);
            this.pendingUpdate = undefined;
        } else {
            webviewView.webview.html = this.getLoadingHtml('Select an object to view details');
        }
    }

    async update(catalog: string, database: string, object: string, type: 'table' | 'view') {
        if (!this._view) {
            this.pendingUpdate = { catalog, database, object, type };
            return;
        }

        this._view.show?.(true); // Bring to focus
        this._view.webview.html = this.getLoadingHtml(`Loading details for ${object}...`);

        try {
            const handle = await this.sessionManager.getActiveSessionHandle();

            // Parallel fetch for speed
            const [descRows, createRows] = await Promise.all([
                this.client.runQuery(handle, `DESCRIBE \`${catalog}\`.\`${database}\`.\`${object}\``),
                this.client.runQuery(handle, `SHOW CREATE ${type === 'view' ? 'VIEW' : 'TABLE'} \`${catalog}\`.\`${database}\`.\`${object}\``)
            ]);

            const createStmt = createRows.length > 0 && createRows[0].fields ? createRows[0].fields[0] : 'No definition found';

            this._view.webview.html = this.getDetailsHtml(object, type, descRows, createStmt);

        } catch (e: any) {
            Logger.error('Failed to fetch object details', e);
            if (this._view) {
                this._view.webview.html = this.getErrorHtml(e.message);
            }
        }
    }

    private getLoadingHtml(message: string): string {
        return `<!DOCTYPE html>
            <html lang="en">
            <head>
                <style>
                    body { font-family: var(--vscode-font-family); padding: 20px; text-align: center; color: var(--vscode-descriptionForeground); }
                </style>
            </head>
            <body>
                <p>${message}</p>
            </body>
            </html>`;
    }

    private getErrorHtml(error: string): string {
        return `<!DOCTYPE html>
           <html lang="en">
           <body>
               <h3 style="color: var(--vscode-errorForeground)">Error Fetching Details</h3>
               <p>${error}</p>
           </body>
           </html>`;
    }

    private getDetailsHtml(name: string, type: string, descRows: any[], createStmt: string): string {
        const rows = descRows.map(r => {
            // Describe returns: name, type, null, key, extras, watermark
            // We adjust based on actual return. Usually: name (0), type (1), ...
            const colName = r.fields[0];
            const colType = r.fields[1];
            const nullable = r.fields[2];
            const key = r.fields[3];
            return `<tr>
                <td>${colName}</td>
                <td><span class="type">${colType}</span></td>
                <td>${nullable}</td>
                <td>${key}</td>
             </tr>`;
        }).join('');

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        :root {
            --bg: var(--vscode-editor-background);
            --fg: var(--vscode-editor-foreground);
            --border: var(--vscode-widget-border);
            --accent: var(--vscode-textLink-foreground);
        }
        body {
            font-family: var(--vscode-editor-font-family);
            font-size: var(--vscode-editor-font-size);
            color: var(--fg);
            background: var(--bg);
            padding: 0; margin: 0;
        } 
        .container { padding: 15px; }
        h2 { margin-top: 0; display: flex; align-items: center; gap: 8px; font-size: 1.2em;}
        .badge { 
            background: var(--vscode-badge-background); 
            color: var(--vscode-badge-foreground);
            padding: 2px 6px; 
            border-radius: 4px; 
            font-size: 0.7em; 
            text-transform: uppercase;
        }

        /* Tabs */
        .tabs { display: flex; border-bottom: 1px solid var(--border); margin-bottom: 15px; }
        .tab { 
            padding: 8px 12px; 
            cursor: pointer; 
            border-bottom: 2px solid transparent; 
            opacity: 0.7;
        }
        .tab:hover { opacity: 1; }
        .tab.active { 
            border-bottom-color: var(--accent); 
            opacity: 1; 
            font-weight: bold; 
        }

        .content { display: none; }
        .content.active { display: block; }

        /* Table */
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { text-align: left; padding: 6px; border-bottom: 1px solid var(--border); opacity: 0.7; }
        td { padding: 6px; border-bottom: 1px solid var(--border); }
        .type { color: var(--accent); font-family: monospace; }
        
        pre { 
            background: var(--vscode-textBlockQuote-background); 
            padding: 10px; 
            overflow-x: auto; 
            border-radius: 4px;
            font-family: var(--vscode-editor-font-family); 
            white-space: pre-wrap;
        }
        

    </style>
</head>
<body>
    <div class="container">
        <h2>
            ${name} 
            <span class="badge">${type}</span>
        </h2>

        <div class="tabs">
            <div class="tab active" onclick="showTab('schema')">Schema</div>
            <div class="tab" onclick="showTab('sql')">Definition</div>
        </div>

        <div id="schema" class="content active">
            <table>
                <thead>
                    <tr>
                        <th>Column</th>
                        <th>Type</th>
                        <th>Null</th>
                        <th>Key</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows}
                </tbody>
            </table>
        </div>

        <div id="sql" class="content">
            <pre>${createStmt}</pre>
        </div>
    </div>

    <script>
        const vscode = acquireVsCodeApi();
        function showTab(id) {
            document.querySelectorAll('.content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(id).classList.add('active');
            event.target.classList.add('active');
        }

    </script>
    </script>
    </body>
    </html>`;
    }
}
