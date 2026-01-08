import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { Logger } from './utils/logger';

export class FlinkNotebookController {
    readonly controllerId = 'flink-sql-notebook-controller';
    readonly notebookType = 'flink-sql-notebook';
    readonly label = 'Flink SQL Gateway';
    readonly supportedLanguages = ['apache-flink', 'sql'];

    private readonly _controller: vscode.NotebookController;
    private _executionOrder = 0;
    private _client: FlinkGatewayClient | undefined;

    private readonly _onDidExecute = new vscode.EventEmitter<void>();
    public readonly onDidExecute = this._onDidExecute.event;

    constructor(private context: vscode.ExtensionContext, private sessionManager: SessionManager) {
        this._controller = vscode.notebooks.createNotebookController(
            this.controllerId,
            this.notebookType,
            this.label
        );

        this._controller.supportedLanguages = this.supportedLanguages;
        this._controller.supportsExecutionOrder = true;
        this._controller.executeHandler = this._execute.bind(this);
    }

    dispose() {
        this._controller.dispose();
    }

    resetConnection() {
        this._client = undefined;
    }

    private getClient(): FlinkGatewayClient {
        if (!this._client) {
            const config = vscode.workspace.getConfiguration('flink');
            const gatewayUrl = config.get<string>('gatewayUrl', 'http://localhost:8083');
            this._client = new FlinkGatewayClient(gatewayUrl);
        }
        return this._client;
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
        execution.start(Date.now()); // Keep running time
        let hasCreateStatement = false;

        try {
            const client = this.getClient();
            const sessionHandle = await this._getSession(client, notebook);

            const sql = cell.document.getText();

            // Split by semicolon and filter empty statements
            // Note: This is a simple split and might break if semicolons are inside strings.
            // For a robust implementation, a proper SQL parser/tokenizer is needed.
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
                    const maxReadyRetries = 60; // Wait up to 30 seconds (500ms * 60)

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

                    // Helper to create data resource output
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

                    // Check if this is a streaming query
                    const isStreaming = resultData.isQueryResult === true;
                    const jobId = resultData.jobID; // Track job ID for cancellation

                    // Display initial results
                    await execution.replaceOutput([createOutput(allResults, isStreaming ? { isStreaming: true, isComplete: false } : undefined)]);

                    // For non-streaming batch queries, we may still need to poll until EOS
                    if (!isStreaming && resultData.resultType !== 'EOS') {
                        Logger.info(`[Flink] Non-streaming query with resultType=${resultData.resultType}, polling for completion...`);
                        let token = resultData.nextResultToken ?? 1;
                        let batchRetries = 0;
                        const maxBatchRetries = 120; // Up to 60 seconds

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

                        // Show final results
                        await execution.replaceOutput([createOutput(allResults)]);
                    }

                    if (isStreaming) {

                        let currentToken = 0; // Start from token 0
                        const maxPolls = 1000; // Increased safety limits
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

                            await new Promise(r => setTimeout(r, 1000)); // Poll every second

                            try {
                                currentToken++;
                                const nextData = await client.fetchResults(sessionHandle, statementHandle, currentToken);

                                Logger.info(`[Flink Poll ${pollCount}] Token: ${currentToken}, ResultType: ${nextData.resultType}, Results: ${nextData.results.length}`);

                                // Handle Status
                                const status = nextData.resultType; // 'PAYLOAD', 'EOS', 'ERROR', etc.

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

                                // Check for termination states
                                if (status === 'EOS' || status === 'FINISHED') {
                                    Logger.info('[Flink] Operation FINISHED/EOS.');
                                    break;
                                }
                                // If INITIALIZED or RUNNING or PAYLOAD, continue.

                                if (consecutiveEmpty >= 10 && status === 'PAYLOAD') {
                                    // Optional: keep polling but maybe slow down? 
                                    // For now just continue.
                                }

                            } catch (pollError: any) {
                                // Fail fast for specific critical errors
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
                                    throw pollError; // Escalating error if persistent
                                }
                            }

                            pollCount++;
                        }

                        // Show final result
                        await execution.replaceOutput([createOutput(allResults, { isStreaming: false, isComplete: true })]);
                    }

                } catch (stmtError: any) {
                    // Check for specific friendly errors (including nested causes)
                    const errStr = ((stmtError.message || '') + (stmtError.stack || '') + JSON.stringify(stmtError)).toLowerCase();

                    if (errStr.includes('noresourceavailableexception')) {
                        await execution.appendOutput([
                            new vscode.NotebookCellOutput([
                                vscode.NotebookCellOutputItem.error({
                                    name: 'Resource Error',
                                    message: 'No resources available on the Flink cluster. Please check if TaskManagers are registered and have enough slots.',
                                    // We purposefully hide the original stack trace to keep it clean, 
                                    // as we strictly identified the issue.
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
                        // Default error handling
                        await execution.appendOutput([
                            new vscode.NotebookCellOutput([
                                vscode.NotebookCellOutputItem.error(stmtError)
                            ])
                        ]);
                    }
                    throw stmtError; // Re-throw to catch block to end execution
                }
            }

            execution.end(true, Date.now());

        } catch (err: any) {
            // Error already appended if it was a statement error
            // If it was a general error (e.g. session creation), append it
            // Check if we already appended an error? 
            // Simplified: just end with false.
            execution.end(false, Date.now());
        } finally {
            if (hasCreateStatement) {
                this._onDidExecute.fire();
            }
        }
    }

    private async _getSession(client: FlinkGatewayClient, notebook: vscode.NotebookDocument): Promise<string> {
        return await this.sessionManager.getActiveSessionHandle();
    }
}


