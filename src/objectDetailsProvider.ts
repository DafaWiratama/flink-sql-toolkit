

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

    updateClient(client: FlinkGatewayClient) {
        this.client = client;
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

        webviewView.webview.onDidReceiveMessage(async (message) => {
            try {
                switch (message.command) {
                    case 'copy':
                        await vscode.env.clipboard.writeText(message.text);
                        vscode.window.showInformationMessage('Copied to clipboard');
                        break;
                    case 'script_select':
                        const editor = vscode.window.activeTextEditor;
                        if (editor) {
                            const snippet = new vscode.SnippetString(message.text + '\n');
                            await editor.insertSnippet(snippet);
                            vscode.window.showInformationMessage('Script inserted');
                        } else {
                            vscode.window.showWarningMessage('No active editor to insert script');
                        }
                        break;
                }
            } catch (error: any) {
                vscode.window.showErrorMessage(`Action failed: ${error.message}`);
            }
        });

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

            this._view.webview.html = this.getDetailsHtml(catalog, database, object, type, descRows, createStmt);

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
                    .spinner { animation: spin 1s linear infinite; height: 20px; width: 20px; border: 2px solid var(--vscode-descriptionForeground); border-top-color: transparent; border-radius: 50%; display: inline-block; margin-bottom: 10px; }
                    @keyframes spin { 100% { transform: rotate(360deg); } }
                </style>
            </head>
            <body>
                <div class="spinner"></div>
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

    private getDetailsHtml(catalog: string, database: string, name: string, type: string, descRows: any[], createStmt: string): string {
        const rows = descRows.map(r => {
            // Describe returns: name, type, null, key, extras, watermark
            const colName = r.fields[0];
            const colType = r.fields[1];
            const nullable = r.fields[2];
            const key = r.fields[3];

            // formatting nullable
            const nullBadge = nullable === 'false' || nullable === false
                ? '<span class="tag tag-req">NOT NULL</span>'
                : '<span class="tag tag-opt">NULL</span>';

            // formatting key
            const keyBadge = (key && key !== 'null' && key !== '')
                ? `<span class="tag tag-key" title="${key}">KEY</span>`
                : '';

            return `<tr>
                <td class="col-name">${colName}</td>
                <td><span class="type">${colType}</span></td>
                <td>${nullBadge}</td>
                <td>${keyBadge}</td>
             </tr>`;
        }).join('');

        const scriptSql = `SELECT * FROM \`${catalog}\`.\`${database}\`.\`${name}\` LIMIT 100;`;

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="${vscode.Uri.file('') /* Just to properly close tag if needed, but not using external CSS here */}" rel="stylesheet">
    <style>
        :root {
            --bg: var(--vscode-editor-background);
            --fg: var(--vscode-editor-foreground);
            --border: var(--vscode-widget-border);
            --accent: var(--vscode-textLink-foreground);
            --hover: var(--vscode-list-hoverBackground);
            --code-bg: var(--vscode-textBlockQuote-background);
        }
        body {
            font-family: var(--vscode-editor-font-family);
            font-size: var(--vscode-editor-font-size);
            color: var(--fg);
            background: var(--bg);
            padding: 0; margin: 0;
            overflow-x: hidden;
        } 
        .container { padding: 15px; }
        
        /* Header */
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border);
        }
        h2 { 
            margin: 0; 
            display: flex; 
            align-items: center; 
            gap: 10px; 
            font-size: 1.1em;
            word-break: break-all;
        }
        .meta-info {
           font-size: 0.8em;
           color: var(--vscode-descriptionForeground);
           margin-top: 4px;
        }
        
        .type-badge { 
            background: var(--vscode-badge-background); 
            color: var(--vscode-badge-foreground);
            padding: 2px 6px; 
            border-radius: 4px; 
            font-size: 0.7em; 
            text-transform: uppercase;
            font-weight: 600;
        }

        /* Actions Bar */
        .actions {
            display: flex;
            gap: 8px;
            margin-bottom: 15px;
        }
        button {
            background: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border: none;
            padding: 6px 12px;
            cursor: pointer;
            border-radius: 2px;
            display: flex;
            align-items: center;
            gap: 6px;
            font-family: var(--vscode-font-family);
            font-size: 0.9em;
        }
        button:hover {
            background: var(--vscode-button-secondaryHoverBackground);
        }
        button.primary {
            background: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
        }
        button.primary:hover {
            background: var(--vscode-button-hoverBackground);
        }
        .icon { font-size: 1.1em; }

        /* Tabs */
        .tabs { display: flex; border-bottom: 1px solid var(--border); margin-bottom: 10px; }
        .tab { 
            padding: 8px 12px; 
            cursor: pointer; 
            border-bottom: 2px solid transparent; 
            opacity: 0.8;
            font-size: 0.9em;
        }
        .tab:hover { opacity: 1; background: var(--hover); }
        .tab.active { 
            border-bottom-color: var(--accent); 
            opacity: 1; 
            font-weight: bold; 
            color: var(--accent);
        }

        .content { display: none; animation: fadein 0.2s; }
        .content.active { display: block; }
        @keyframes fadein { from { opacity: 0; } to { opacity: 1; } }

        /* Table */
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { 
            text-align: left; 
            padding: 8px; 
            border-bottom: 1px solid var(--border); 
            color: var(--vscode-descriptionForeground);
            font-weight: 600;
        }
        td { 
            padding: 8px; 
            border-bottom: 1px solid var(--border); 
            vertical-align: middle;
        }
        tr:last-child td { border-bottom: none; }
        
        .col-name { font-weight: 600; }
        .type { color: var(--accent); font-family: var(--vscode-editor-font-family); font-size: 0.95em; }
        
        /* Tags */
        .tag { font-size: 0.75em; padding: 2px 5px; border-radius: 3px; font-weight: 500; }
        .tag-req { background: var(--vscode-inputValidation-errorBackground); color: var(--vscode-inputValidation-errorForeground); border: 1px solid var(--vscode-inputValidation-errorBorder); opacity: 0.8; }
        .tag-opt { color: var(--vscode-descriptionForeground); border: 1px solid var(--border); }
        .tag-key { background: var(--vscode-list-highlightForeground); color: var(--bg); font-weight: bold; }

        /* Code Block */
        .code-container {
            position: relative;
            background: var(--code-bg);
            border-radius: 4px;
            border: 1px solid var(--border);
        }
        pre { 
            padding: 12px; 
            margin: 0;
            overflow-x: auto; 
            font-family: var(--vscode-editor-font-family); 
            white-space: pre-wrap;
            font-size: 0.9em;
            line-height: 1.5;
        }
        .copy-btn-floating {
            position: absolute;
            top: 5px;
            right: 5px;
            background: var(--bg);
            opacity: 0.7;
            padding: 4px 8px;
            font-size: 0.8em;
        }
        .copy-btn-floating:hover { opacity: 1; }

    </style>
</head>
<body>
    <div class="container">
        
        <header>
            <div>
                <h2>${name} <span class="type-badge">${type}</span></h2>
                <div class="meta-info">${catalog} • ${database}</div>
            </div>
        </header>

        <div class="actions">
            <button class="primary" onclick="scriptSelect()">
                <span class="icon">▶</span> Script SELECT
            </button>
            <button onclick="copyDefinition()">
                <span class="icon" style="display: flex; align-items: center;">
                    <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                        <path fill-rule="evenodd" clip-rule="evenodd" d="M4 4l1-1h5.414L14 6.586V14l-1 1H5l-1-1V4zm9 3l-3-3H5v10h8V7z"/>
                        <path fill-rule="evenodd" clip-rule="evenodd" d="M3 1L2 2v10l1 1V2h6.414l-1-1H3z"/>
                    </svg>
                </span>
                Copy DDL
            </button>
        </div>

        <div class="tabs">
            <div id="tab-schema" class="tab active" onclick="showTab('schema')">Schema</div>
            <div id="tab-sql" class="tab" onclick="showTab('sql')">Definition</div>
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
            <div class="code-container">
                <pre id="ddl-content">${createStmt}</pre>
            </div>
        </div>
    </div>

    <script>
        const vscode = acquireVsCodeApi();
        
        const scriptText = ${JSON.stringify(scriptSql)};
        const ddlText = ${JSON.stringify(createStmt)};

        function showTab(id) {
            document.querySelectorAll('.content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            
            document.getElementById(id).classList.add('active');
            document.getElementById('tab-' + id).classList.add('active');
        }

        function scriptSelect() {
            vscode.postMessage({
                command: 'script_select',
                text: scriptText
            });
        }

        function copyDefinition() {
            vscode.postMessage({
                command: 'copy',
                text: ddlText
            });
        }
    </script>
</body>
</html>`;
    }
}
