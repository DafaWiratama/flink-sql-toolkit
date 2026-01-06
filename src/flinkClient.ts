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
                throw new Error(`Flink Gateway Error (${response.status}): ${text}`);
            }

            return await response.json();
        } catch (error: any) {
            throw new Error(`Failed to request Flink Gateway: ${error.message}`);
        }
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

    // --- Metadata API (Compat Layer) ---
    // Uses SQL internally because REST Metadata endpoints are not available on all Gateway versions.


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
        const rows = await this.executeMetadataSql(sessionHandle, 'SHOW CATALOGS');
        return rows.map(r => this.getValue(r, 0));
    }

    async listDatabases(sessionHandle: string, catalog: string): Promise<string[]> {
        const rows = await this.executeMetadataSql(sessionHandle, `SHOW DATABASES IN \`${catalog}\``);
        return rows.map(r => this.getValue(r, 0));
    }

    async listTables(sessionHandle: string, catalog: string, database: string): Promise<string[]> {
        const rows = await this.executeMetadataSql(sessionHandle, `SHOW TABLES IN \`${catalog}\`.\`${database}\``);
        return rows.map(r => this.getValue(r, 0));
    }

    async listTablesCurrent(sessionHandle: string): Promise<string[]> {
        const rows = await this.executeMetadataSql(sessionHandle, 'SHOW TABLES');
        return rows.map(r => this.getValue(r, 0));
    }

    async listViews(sessionHandle: string, catalog: string, database: string): Promise<string[]> {
        const rows = await this.executeMetadataSql(sessionHandle, `SHOW VIEWS IN \`${catalog}\`.\`${database}\``);
        return rows.map(r => this.getValue(r, 0));
    }

    async getTable(sessionHandle: string, catalog: string, database: string, table: string): Promise<any> {
        const rows = await this.executeMetadataSql(sessionHandle, `DESCRIBE \`${catalog}\`.\`${database}\`.\`${table}\``);
        // DESCRIBE output: name, type, ...
        const columns = rows.map(r => ({
            name: this.getValue(r, 0),
            dataType: this.getValue(r, 1)
        }));
        return { schema: { columns } };
    }
}
