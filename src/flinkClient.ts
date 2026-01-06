import * as vscode from 'vscode';
import { Logger } from './utils/logger';

export interface FlinkSession {
    sessionHandle: string;
}

export interface ExecuteResult {
    statementHandle: string;
}

export interface ResultColumn {
    name: string;
    logicalType: any;
    comment?: string;
}

export interface ResultData {
    results: any[];
    columns: ResultColumn[];
    resultKind: string;
    resultType: string;
    jobID?: string;
    nextResultUri?: string;
    isQueryResult: boolean;
    nextResultToken?: number;
}



// Custom Error class to carry Flink server-side stack traces
export class FlinkServerError extends Error {
    constructor(message: string, stack?: string, public statusCode?: number) {
        super(message);
        this.name = 'Flink Server Error';
        if (stack) {
            this.stack = stack;
        }
    }
}

export class FlinkGatewayClient {
    private baseUrl: string;
    private jobManagerUrl: string;

    constructor(baseUrl: string, jobManagerUrl: string = 'http://localhost:8081') {
        this.baseUrl = baseUrl.replace(/\/$/, ''); // Remove trailing slash
        this.jobManagerUrl = jobManagerUrl.replace(/\/$/, '');
    }

    private async request(endpoint: string, method: string = 'GET', body?: any): Promise<any> {
        const headers: any = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        };

        try {
            const response = await fetch(`${this.baseUrl}${endpoint}`, {
                method,
                headers,
                body: body ? JSON.stringify(body) : undefined
            });

            if (!response.ok) {
                const text = await response.text();
                this.handleErrorResponse(response.status, text);
            }

            return await response.json();
        } catch (error: any) {
            if (error instanceof FlinkServerError) {
                throw error;
            }
            throw new Error(`Failed to request Flink Gateway: ${error.message}`);
        }
    }

    private handleErrorResponse(status: number, text: string): void {
        let message = `Flink Gateway Error (${status})`;
        let stack: string | undefined = undefined;

        try {
            const data = JSON.parse(text);
            if (data.errors && Array.isArray(data.errors)) {
                // Primary error message
                // data.errors[0] is usually "Internal server error." or similar short msg
                if (data.errors.length > 0) {
                    message = data.errors[0];
                }

                // Detailed stack trace is usually in data.errors[1]
                if (data.errors.length > 1) {
                    let rawStack = data.errors[1];
                    // Clean up Flink's wrapping format
                    // e.g. <Exception on server side:\n ... >
                    rawStack = rawStack.replace(/^<Exception on server side:\n?/, '').replace(/>$/, '');
                    stack = rawStack;

                    // Extract "Caused by" for the friendly message
                    const lines = rawStack.split('\n');
                    const causedByLines = lines.filter((l: string) => l.trim().startsWith('Caused by:'));

                    if (causedByLines.length > 0) {
                        // Use the most specific cause (last one) to give the user immediate insight
                        const rootCause = causedByLines[causedByLines.length - 1].trim();
                        // Append to message
                        message += ` ${rootCause}`;
                    }
                }
            } else {
                // Valid JSON but not the expected error format
                message += `: ${text}`;
            }
        } catch {
            // Not JSON, just raw text (maybe 404 html or plain text)
            // Limit length if too long?
            if (text.length > 500) {
                message += `: ${text.substring(0, 500)}...`;
            } else {
                message += `: ${text}`;
            }
        }

        throw new FlinkServerError(message, stack, status);
    }

    async createSession(sessionName: string): Promise<FlinkSession> {
        const result = await this.request('/sessions', 'POST', {
            sessionName,
            properties: {}
        });
        return { sessionHandle: result.sessionHandle };
    }

    async checkSession(sessionHandle: string): Promise<boolean> {
        try {
            await this.request(`/sessions/${sessionHandle}`, 'GET');
            return true;
        } catch {
            return false;
        }
    }

    async executeStatement(sessionHandle: string, statement: string): Promise<ExecuteResult> {
        const result = await this.request(`/sessions/${sessionHandle}/statements`, 'POST', {
            statement,
            executionTimeout: 0 // Wait indefinitely (or meaningful default)
        });
        return { statementHandle: result.operationHandle };
    }

    // Polling is complex in Flink Gateway, usually we fetch results until 'EOS' or similar.
    // For simplicity, we just fetch one page or the available results.
    async fetchResults(sessionHandle: string, statementHandle: string, token: number = 0): Promise<ResultData> {
        const result = await this.request(`/sessions/${sessionHandle}/operations/${statementHandle}/result/${token}`, 'GET');

        // Debug: Log raw API response - reduced noise
        // Logger.info('[Flink API Response]', JSON.stringify(result, null, 2));

        // This mapping depends on the exact Flink Gateway API version response structure.
        // Assuming standard structure:
        return {
            results: result.results ? result.results.data : [],
            columns: result.results ? result.results.columns : [],
            resultKind: result.resultKind,
            resultType: result.resultType || 'PAYLOAD',
            jobID: result.jobID,
            nextResultUri: result.nextResultUri,
            isQueryResult: result.isQueryResult || false,
            nextResultToken: result.nextResultToken
        };
    }

    async cancelJob(jobId: string): Promise<void> {
        try {
            const response = await fetch(`${this.jobManagerUrl}/jobs/${jobId}?mode=cancel`, {
                method: 'PATCH'
            });

            if (response.ok) {
                Logger.info('[Flink JobManager] Job cancelled:', jobId);
            } else {
                console.warn('[Flink JobManager] Failed to cancel job:', await response.text());
            }
        } catch (error: any) {
            console.warn('[Flink JobManager] Error cancelling job:', error.message);
        }
    }

    async getTaskManagers(): Promise<any[] | null> {
        try {
            const response = await fetch(`${this.jobManagerUrl}/taskmanagers`);
            if (!response.ok) {
                console.warn('[Flink JobManager] Failed to fetch taskmanagers:', response.statusText);
                return null;
            }
            const data: any = await response.json();
            return data.taskmanagers || [];
        } catch (error: any) {
            // If fetch fails (e.g. connection refused), return null to indicate offline
            console.warn('[Flink JobManager] Error fetching taskmanagers:', error.message);
            return null;
        }
    }

    async getClusterOverview(): Promise<any | null> {
        try {
            const response = await fetch(`${this.jobManagerUrl}/overview`);
            if (!response.ok) {
                console.warn('[Flink JobManager] Failed to fetch overview:', response.statusText);
                return null;
            }
            return await response.json();
        } catch (error: any) {
            console.warn('[Flink JobManager] Error fetching overview:', error.message);
            return null;
        }
    }

    async getJobs(): Promise<any[] | null> {
        try {
            const response = await fetch(`${this.jobManagerUrl}/jobs/overview`);
            if (!response.ok) {
                // Fallback to /jobs if overview is missing (older Flink?)
                const response2 = await fetch(`${this.jobManagerUrl}/jobs`);
                if (!response2.ok) {
                    console.warn('[Flink JobManager] Failed to fetch jobs:', response.statusText);
                    return null;
                }
                const data2: any = await response2.json();
                return data2.jobs || [];
            }
            const data: any = await response.json();
            return data.jobs || [];
        } catch (error: any) {
            console.warn('[Flink JobManager] Error fetching jobs:', error.message);
            return null;
        }
    }

    async getJobDetails(jobId: string): Promise<any> {
        try {
            const response = await fetch(`${this.jobManagerUrl}/jobs/${jobId}`);
            if (!response.ok) {
                Logger.warn(`[Flink JobManager] Failed to fetch details for job ${jobId}: ${await response.text()}`);
                return null;
            }
            return await response.json();
        } catch (error: any) {
            Logger.error(`[Flink JobManager] Error fetching details for job ${jobId}:`, error);
            return null;
        }
    }

    async getJobPlan(jobId: string): Promise<any> {
        try {
            const response = await fetch(`${this.jobManagerUrl}/jobs/${jobId}/plan`);
            if (!response.ok) {
                Logger.warn(`[Flink JobManager] Failed to fetch plan for job ${jobId}: ${await response.text()}`);
                return null;
            }
            return await response.json();
        } catch (error: any) {
            Logger.error(`[Flink JobManager] Error fetching plan for job ${jobId}:`, error);
            return null;
        }
    }

    async cancelOperation(sessionHandle: string, statementHandle: string, jobId?: string): Promise<void> {
        // First, try to cancel the actual Flink job if we have the jobId
        if (jobId) {
            await this.cancelJob(jobId);
        }

        try {
            // Try to cancel the operation (stops the job)
            await this.request(`/sessions/${sessionHandle}/operations/${statementHandle}/cancel`, 'POST');
            Logger.info('[Flink] Operation cancelled:', statementHandle);

            // Also close the operation to release resources
            try {
                await this.request(`/sessions/${sessionHandle}/operations/${statementHandle}/close`, 'DELETE');
                Logger.info('[Flink] Operation closed');
            } catch (closeError) {
                // Closing might fail if already closed, that's ok
            }
        } catch (error: any) {
            // Check if operation is already finished - this is fine, not an error
            if (error.message.includes('FINISHED') || error.message.includes('CANCELED')) {
                Logger.info('[Flink] Operation already finished, no cancellation needed');
            } else {
                console.warn('[Flink] Failed to cancel operation:', error.message);
            }
            // Don't throw - best effort cancellation
        }
    }

    // --- Metadata API (REST) ---
    // Uses Flink 1.20 compatible REST endpoints, falls back to SQL if needed.

    public async runQuery(sessionHandle: string, sql: string): Promise<any[]> {
        return this.executeMetadataSql(sessionHandle, sql);
    }

    private async executeMetadataSql(sessionHandle: string, sql: string): Promise<any[]> {
        const { statementHandle } = await this.executeStatement(sessionHandle, sql);

        // Metadata queries (SHOW TABLES, etc) should be instant.
        // We poll immediately once.
        let result = await this.fetchResults(sessionHandle, statementHandle, 0);

        // If not ready immediately, fast retry loop (max 1 second)
        let retries = 0;
        while (result.resultType !== 'EOS' && retries < 20) {
            await new Promise(r => setTimeout(r, 50)); // 50ms interval
            if (result.nextResultToken !== undefined) {
                result = await this.fetchResults(sessionHandle, statementHandle, result.nextResultToken);
            } else {
                result = await this.fetchResults(sessionHandle, statementHandle, 0);
            }
            retries++;
        }

        // Return whatever we have. For metadata, we expect 1 page.
        // Deduplication for safety
        const uniqueRows: any[] = [];
        const seen = new Set<string>();
        if (result.results) {
            for (const row of result.results) {
                const key = JSON.stringify(row);
                if (!seen.has(key)) {
                    seen.add(key);
                    uniqueRows.push(row);
                }
            }
        }
        return uniqueRows;
    }

    private getValue(row: any, index: number): string {
        if (row && row.fields && Array.isArray(row.fields)) {
            return String(row.fields[index]);
        }
        if (Array.isArray(row)) {
            return String(row[index]);
        }
        return String(row); // fallback
    }

    async listCatalogs(sessionHandle: string): Promise<string[]> {
        return this.listCatalogsSql(sessionHandle);
    }

    private async listCatalogsSql(sessionHandle: string): Promise<string[]> {
        const rows = await this.executeMetadataSql(sessionHandle, 'SHOW CATALOGS');
        return rows.map(r => this.getValue(r, 0));
    }

    async listDatabases(sessionHandle: string, catalog: string): Promise<string[]> {
        return this.listDatabasesSql(sessionHandle, catalog);
    }

    private async listDatabasesSql(sessionHandle: string, catalog: string): Promise<string[]> {
        const rows = await this.executeMetadataSql(sessionHandle, `SHOW DATABASES IN \`${catalog}\``);
        return rows.map(r => this.getValue(r, 0));
    }

    /**
     * Sets the current catalog context.
     */
    async useCatalog(sessionHandle: string, catalog: string): Promise<void> {
        await this.executeMetadataSql(sessionHandle, `USE CATALOG \`${catalog}\``);
    }

    /**
     * Sets the current database context.
     */
    async useDatabase(sessionHandle: string, database: string): Promise<void> {
        await this.executeMetadataSql(sessionHandle, `USE \`${database}\``);
    }

    /**
     * Lists all objects (tables and views) with their kind.
     * Uses SHOW TABLES and SHOW VIEWS from current session context.
     */
    async listTablesWithKind(sessionHandle: string): Promise<{ name: string, kind: string }[]> {
        const tables = await this.listTables(sessionHandle);
        const views = await this.listViews(sessionHandle);

        // Create a set of view names for filtering (case-insensitive)
        const viewSet = new Set(views.map(v => v.toLowerCase()));

        const result: { name: string, kind: string }[] = [];

        // Tables = items from SHOW TABLES that are NOT in SHOW VIEWS
        for (const t of tables) {
            if (!viewSet.has(t.toLowerCase())) {
                result.push({ name: t, kind: 'TABLE' });
            }
        }

        // Views = items from SHOW VIEWS
        for (const v of views) {
            result.push({ name: v, kind: 'VIEW' });
        }

        return result;
    }

    /**
     * Lists tables from the current session context.
     */
    async listTables(sessionHandle: string): Promise<string[]> {
        try {
            const rows = await this.executeMetadataSql(sessionHandle, 'SHOW TABLES');
            return rows.map(r => this.getValue(r, 0));
        } catch (e) {
            Logger.warn('[FlinkClient] SHOW TABLES failed:', e);
            return [];
        }
    }

    /**
     * Lists views from the current session context.
     */
    async listViews(sessionHandle: string): Promise<string[]> {
        try {
            const rows = await this.executeMetadataSql(sessionHandle, 'SHOW VIEWS');
            return rows.map(r => this.getValue(r, 0));
        } catch (e) {
            Logger.warn('[FlinkClient] SHOW VIEWS failed:', e);
            return [];
        }
    }

    /**
     * Gets table/view schema using DESCRIBE.
     */
    async getTableSchema(sessionHandle: string, catalog: string, database: string, table: string): Promise<{ name: string, dataType: string }[]> {
        const rows = await this.executeMetadataSql(sessionHandle, `DESCRIBE \`${catalog}\`.\`${database}\`.\`${table}\``);
        return rows.map(r => ({
            name: this.getValue(r, 0),
            dataType: this.getValue(r, 1)
        }));
    }

    // Legacy method for compatibility
    async getTable(sessionHandle: string, catalog: string, database: string, table: string): Promise<any> {
        const columns = await this.getTableSchema(sessionHandle, catalog, database, table);
        return { schema: { columns } };
    }
}

