import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';
import { Logger } from './utils/logger';

interface JobData {
    jobId: string;
    jobName: string;
    status: string;
    startTime: string;
}

export class FlinkJobsProvider implements vscode.TreeDataProvider<JobTreeItem>, vscode.Disposable {
    private _onDidChangeTreeData: vscode.EventEmitter<JobTreeItem | undefined | null | void> = new vscode.EventEmitter<JobTreeItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<JobTreeItem | undefined | null | void> = this._onDidChangeTreeData.event;

    private client: FlinkGatewayClient;
    private jobs: JobData[] = [];
    private timer: NodeJS.Timeout | undefined;

    constructor(gatewayUrl: string, jobManagerUrl: string, private mode: 'RUNNING' | 'HISTORY') {
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        this.startPolling();
    }

    updateConnection(gatewayUrl: string, jobManagerUrl: string) {
        this.client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
        this.refresh();
    }

    dispose() {
        this.stopPolling();
    }

    private startPolling() {
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
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: JobTreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: JobTreeItem): vscode.ProviderResult<JobTreeItem[]> {
        if (element) {
            return []; // Flat list, no children
        }

        return this.fetchJobs().then(() => {
            let filteredJobs: JobData[] = [];

            if (this.mode === 'RUNNING') {
                filteredJobs = this.jobs.filter(j => j.status === 'RUNNING' || j.status === 'CREATED' || j.status === 'RESTARTING');
            } else {
                // History
                filteredJobs = this.jobs.filter(j => j.status !== 'RUNNING' && j.status !== 'CREATED' && j.status !== 'RESTARTING');
                filteredJobs.sort((a, b) => {
                    const timeA = new Date(a.startTime).getTime();
                    const timeB = new Date(b.startTime).getTime();
                    return timeB - timeA; // Descending
                });
            }

            return filteredJobs.map(job => this.createJobItem(job));
        });
    }

    private createJobItem(job: JobData): JobTreeItem {
        const icon = this.getStatusIcon(job.status);
        const item = new JobTreeItem(
            job.jobName,
            job.status,
            vscode.TreeItemCollapsibleState.None, // Leaf node
            job,
            icon
        );
        return item;
    }

    private async fetchJobs(): Promise<void> {
        try {
            const rawJobs = await this.client.getJobs();

            if (rawJobs === null) {
                // Connection failed / Offline
                this.jobs = [{
                    jobId: 'offline',
                    jobName: 'JobManager Offline',
                    status: 'OFFLINE',
                    startTime: ''
                }];
                return;
            }

            this.jobs = rawJobs.map((j: any) => ({
                jobId: j.jid,
                jobName: j.name,
                status: j.state,
                startTime: this.formatTime(j['start-time'])
            }));

            if (this.jobs.length === 0) {
                // Empty state if needed, or just empty list
            }

        } catch (error: any) {
            Logger.error('[Flink Jobs] Failed to fetch jobs:', error.message);
            // Fallback to offline state
            this.jobs = [{
                jobId: 'offline',
                jobName: 'JobManager Offline',
                status: 'OFFLINE',
                startTime: ''
            }];
        }
    }

    private formatTime(ts: number | string): string {
        if (!ts) { return ''; }
        const date = new Date(ts);
        return date.toLocaleString();
    }

    private getStatusIcon(status: string): vscode.ThemeIcon {
        switch (status.toUpperCase()) {
            case 'RUNNING': return new vscode.ThemeIcon('gear~spin');
            case 'FINISHED': return new vscode.ThemeIcon('check');
            case 'FAILED': return new vscode.ThemeIcon('error');
            case 'CANCELED': return new vscode.ThemeIcon('circle-slash');
            case 'SUSPENDED': return new vscode.ThemeIcon('debug-pause');
            case 'OFFLINE': return new vscode.ThemeIcon('bracket-error');
            default: return new vscode.ThemeIcon('circle-outline');
        }
    }

    public async cancelJob(item: JobTreeItem): Promise<void> {
        if (!item.jobData) { return; }

        const answer = await vscode.window.showWarningMessage(
            `Are you sure you want to stop job "${item.jobData.jobName}"?`,
            { modal: true },
            'Stop Job'
        );

        if (answer === 'Stop Job') {
            await this.client.cancelJob(item.jobData.jobId);
            vscode.window.showInformationMessage('Job stop requested.');
            setTimeout(() => this.refresh(), 1000);
        }
    }
}

class JobTreeItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly status: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly jobData: JobData,
        icon?: vscode.ThemeIcon
    ) {
        super(label, collapsibleState);

        if (icon) {
            this.iconPath = icon;
        }

        this.tooltip = `Job ID: ${jobData.jobId}\nStatus: ${jobData.status}\nStart Time: ${jobData.startTime}`;
        this.description = jobData.status; // Show status next to name
        this.contextValue = jobData.status === 'RUNNING' ? 'flink-job-running' : 'flink-job-history';

        // Command to open details
        if (jobData.status !== 'OFFLINE') {
            this.command = {
                command: 'flink.showJobDetail',
                title: 'Show Job Details',
                arguments: [jobData.jobId, jobData.status]
            };
        }
    }
}
