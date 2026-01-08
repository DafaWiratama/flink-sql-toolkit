import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager, SessionInfo } from './sessionManager';
import { ConnectionManager } from './connectionManager';
import { Logger } from './utils/logger';

export class FlinkNotebookController implements vscode.Disposable {
    readonly controllerId: string;
    readonly notebookType = 'flink-sql-notebook';
    readonly label: string;
    readonly supportedLanguages = ['apache-flink', 'sql'];

    private readonly _controller: vscode.NotebookController;
    private _executionOrder = 0;

    private readonly _onDidExecute = new vscode.EventEmitter<void>();
    public readonly onDidExecute = this._onDidExecute.event;

    constructor(
        private context: vscode.ExtensionContext,
        private connectionManager: ConnectionManager,
        private sessionManager: SessionManager,
        private session: SessionInfo
    ) {
        // Get connection name for label
        const conn = connectionManager.getConnection(session.connectionId);
        const connName = conn?.name || 'Unknown';

        this.controllerId = `flink-sql-notebook-${session.handle}`;
        this.label = `Flink: ${session.name} @ ${connName}`;

        this._controller = vscode.notebooks.createNotebookController(
            this.controllerId,
            this.notebookType,
            this.label
        );

        this._controller.supportedLanguages = this.supportedLanguages;
        this._controller.supportsExecutionOrder = true;
        this._controller.executeHandler = this._execute.bind(this);

        // Show connection info in description
        this._controller.description = conn?.gatewayUrl || 'No connection';
    }

    dispose() {
        this._controller.dispose();
        this._onDidExecute.dispose();
    }

    getSession(): SessionInfo {
        return this.session;
    }

    private getClient(): FlinkGatewayClient {
        // Get the client from the session's connection
        const client = this.sessionManager.getClientForSession(this.session.handle);
        if (!client) {
            throw new Error(`No connection found for session ${this.session.name}`);
        }
        return client;
    }

    private async _execute(
        cells: vscode.NotebookCell[],
        _notebook: vscode.NotebookDocument,
        _controller: vscode.NotebookController
    ): Promise<void> {
        for (const cell of cells) {
            await this._doExecution(cell, _notebook);
        }
    }

    private async _doExecution(cell: vscode.NotebookCell, notebook: vscode.NotebookDocument): Promise<void> {
        const execution = this._controller.createNotebookCellExecution(cell);
        execution.executionOrder = ++this._executionOrder;
        execution.start(Date.now());
        let hasCreateStatement = false;

        try {
            const client = this.getClient();
            // Use this controller's session handle directly
            const sessionHandle = this.session.handle;

            const sql = cell.document.getText();

            // Split by semicolon and filter empty statements
            const statements = sql.split(';')
                .map(s => s.trim())
                .filter(s => s.length > 0);

            hasCreateStatement = statements.some(s => s.toUpperCase().startsWith('CREATE'));

            if (statements.length === 0) {
                execution.end(true, Date.now());
                return;
            }

            // Clear previous outputs
            await execution.clearOutput();

            for (const statement of statements) {
                try {
                    const { statementHandle } = await client.executeStatement(sessionHandle, statement);

                    // Wait for results to be ready (poll for NOT_READY -> PAYLOAD/EOS)
                    let resultData = await client.fetchResults(sessionHandle, statementHandle, 0);
                    let readyRetries = 0;
                    const maxReadyRetries = 60;

                    while (resultData.resultType === 'NOT_READY' && readyRetries < maxReadyRetries) {
                        if (execution.token.isCancellationRequested) {
                            Logger.info('[Flink] Cancellation requested while waiting for results');
                            break;
                        }
                        await new Promise(r => setTimeout(r, 500));
                        resultData = await client.fetchResults(sessionHandle, statementHandle, 0);
                        readyRetries++;
                        Logger.info(`[Flink] Waiting for results... (attempt ${readyRetries}, type: ${resultData.resultType})`);
                    }

                    if (resultData.resultType === 'NOT_READY') {
                        throw new Error('Query timed out waiting for results. The job may still be running in the background.');
                    }

                    let allResults: any[] = [...resultData.results];
                    const columns = resultData.columns;

                    const createOutput = (rows: any[], streamingInfo?: { isStreaming: boolean; isComplete?: boolean; offset?: number }) => {
                        const items: vscode.NotebookCellOutputItem[] = [];

                        const dataResource = {
                            schema: {
                                fields: columns.map(c => ({
                                    name: c.name,
                                    type: (typeof c.logicalType === 'string' ? c.logicalType : c.logicalType?.type || 'string').toLowerCase()
                                }))
                            },
                            data: rows,
                            metadata: streamingInfo
                        };

                        items.push(vscode.NotebookCellOutputItem.json(dataResource, 'application/x-flink-table'));
                        return new vscode.NotebookCellOutput(items);
                    };

                    const isStreaming = resultData.isQueryResult === true;
                    const jobId = resultData.jobID;

                    await execution.replaceOutput([createOutput(allResults, isStreaming ? { isStreaming: true, isComplete: false } : undefined)]);

                    // For non-streaming batch queries, poll until EOS
                    if (!isStreaming && resultData.resultType !== 'EOS') {
                        Logger.info(`[Flink] Non-streaming query with resultType=${resultData.resultType}, polling for completion...`);
                        let token = resultData.nextResultToken ?? 1;
                        let batchRetries = 0;
                        const maxBatchRetries = 120;

                        while (resultData.resultType !== 'EOS' && batchRetries < maxBatchRetries) {
                            if (execution.token.isCancellationRequested) {
                                Logger.info('[Flink] Cancellation requested during batch polling');
                                await client.cancelOperation(sessionHandle, statementHandle, jobId);
                                break;
                            }
                            await new Promise(r => setTimeout(r, 500));

                            try {
                                const nextData = await client.fetchResults(sessionHandle, statementHandle, token);

                                if (nextData.results && nextData.results.length > 0) {
                                    allResults.push(...nextData.results);
                                    await execution.replaceOutput([createOutput(allResults)]);
                                }

                                Logger.info(`[Flink Batch Poll ${batchRetries}] Token: ${token}, ResultType: ${nextData.resultType}, Results: ${nextData.results.length}`);

                                if (nextData.resultType === 'EOS') {
                                    Logger.info('[Flink] Batch query completed (EOS)');
                                    resultData = nextData;
                                    break;
                                }

                                if (nextData.resultType === 'ERROR') {
                                    throw new Error('Flink batch operation failed with ERROR status.');
                                }

                                token = nextData.nextResultToken ?? token + 1;
                                resultData = nextData;
                            } catch (pollError: any) {
                                Logger.error('[Flink Batch Poll Error]', pollError.message);
                                batchRetries++;
                                if (batchRetries >= 5) {
                                    throw pollError;
                                }
                            }

                            batchRetries++;
                        }

                        await execution.replaceOutput([createOutput(allResults)]);
                    }

                    if (isStreaming) {
                        let currentToken = 0;
                        const maxPolls = 1000;
                        let pollCount = 0;
                        let consecutiveEmpty = 0;
                        let totalRows = allResults.length;

                        while (pollCount < maxPolls) {
                            if (execution.token.isCancellationRequested) {
                                Logger.info('[Flink] Cancellation requested, cancelling job...');
                                await client.cancelOperation(sessionHandle, statementHandle, jobId);
                                await execution.replaceOutput([createOutput(allResults, { isStreaming: false, isComplete: true, offset: Math.max(0, totalRows - allResults.length) })]);
                                break;
                            }

                            await new Promise(r => setTimeout(r, 1000));

                            try {
                                currentToken++;
                                const nextData = await client.fetchResults(sessionHandle, statementHandle, currentToken);

                                Logger.info(`[Flink Poll ${pollCount}] Token: ${currentToken}, ResultType: ${nextData.resultType}, Results: ${nextData.results.length}`);

                                const status = nextData.resultType;

                                if (status === 'ERROR') {
                                    throw new Error('Flink Operation failed with ERROR status.');
                                }

                                if (status === 'CANCELED') {
                                    Logger.info('[Flink] Operation CANCELED.');
                                    break;
                                }

                                if (nextData.results.length > 0) {
                                    consecutiveEmpty = 0;
                                    allResults.push(...nextData.results);
                                    totalRows += nextData.results.length;

                                    const maxStreamingRows = 1000;
                                    if (allResults.length > maxStreamingRows) {
                                        allResults = allResults.slice(-maxStreamingRows);
                                    }

                                    const offset = Math.max(0, totalRows - allResults.length);
                                    await execution.replaceOutput([createOutput(allResults, { isStreaming: true, isComplete: false, offset })]);
                                } else {
                                    consecutiveEmpty++;
                                }

                                if (status === 'EOS' || status === 'FINISHED') {
                                    Logger.info('[Flink] Operation FINISHED/EOS.');
                                    break;
                                }

                            } catch (pollError: any) {
                                const errStr = (pollError.toString() + JSON.stringify(pollError)).toLowerCase();
                                if (errStr.includes('tablealreadyexistexception')) {
                                    throw pollError;
                                }
                                if (errStr.includes('noresourceavailableexception')) {
                                    throw pollError;
                                }

                                Logger.error('[Flink Poll Error]', pollError.message);
                                consecutiveEmpty++;
                                if (consecutiveEmpty >= 5) {
                                    throw pollError;
                                }
                            }

                            pollCount++;
                        }

                        await execution.replaceOutput([createOutput(allResults, { isStreaming: false, isComplete: true })]);
                    }

                } catch (stmtError: any) {
                    const errStr = ((stmtError.message || '') + (stmtError.stack || '') + JSON.stringify(stmtError)).toLowerCase();

                    if (errStr.includes('noresourceavailableexception')) {
                        await execution.appendOutput([
                            new vscode.NotebookCellOutput([
                                vscode.NotebookCellOutputItem.error({
                                    name: 'Resource Error',
                                    message: 'No resources available on the Flink cluster. Please check if TaskManagers are registered and have enough slots.',
                                    stack: undefined
                                })
                            ])
                        ]);
                    } else if (errStr.includes('tablealreadyexistexception') || errStr.includes('table (or view)')) {
                        await execution.appendOutput([
                            new vscode.NotebookCellOutput([
                                vscode.NotebookCellOutputItem.text(`Error: Table already exists.`, 'text/plain')
                            ])
                        ]);
                    } else {
                        await execution.appendOutput([
                            new vscode.NotebookCellOutput([
                                vscode.NotebookCellOutputItem.error(stmtError)
                            ])
                        ]);
                    }
                    throw stmtError;
                }
            }

            execution.end(true, Date.now());

        } catch (err: any) {
            execution.end(false, Date.now());
        } finally {
            if (hasCreateStatement) {
                this._onDidExecute.fire();
            }
        }
    }
}
