import * as vscode from 'vscode';
import { FlinkGatewayClient } from './flinkClient';

export class FlinkStatusBar implements vscode.Disposable {
    private statusBarItem: vscode.StatusBarItem;
    private timer: NodeJS.Timeout | undefined;

    constructor(private client: FlinkGatewayClient) {
        this.statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 99); // Priority 99 to be near other Flink items
        this.statusBarItem.command = 'flinkTaskManagers.refresh'; // Clicking refreshes TM view
        this.statusBarItem.tooltip = 'Flink Task Slots (Active / Total)';
        this.update(); // Initial update
        this.startPolling();
    }

    updateClient(client: FlinkGatewayClient) {
        this.client = client;
        this.update();
    }

    startPolling() {
        this.timer = setInterval(() => this.update(), 10000); // Poll every 10s
    }

    async update() {
        try {
            const tms = await this.client.getTaskManagers();
            if (tms && tms.length > 0) { // Check if tms is not null/undefined and not empty
                const totalSlots = tms.reduce((acc: number, tm: any) => acc + (tm.slotsNumber || 0), 0);
                const freeSlots = tms.reduce((acc: number, tm: any) => acc + (tm.freeSlots || 0), 0);
                const activeSlots = totalSlots - freeSlots;

                this.statusBarItem.text = `$(server) Flink: ${tms.length} TMs (${activeSlots}/${totalSlots} Slots)`;
                this.statusBarItem.tooltip = `Flink Cluster Status\n\nActive Tasks: ${activeSlots}\nFree Slots: ${freeSlots}\nTotal Slots: ${totalSlots}`;
                this.statusBarItem.color = undefined; // Default color
                this.statusBarItem.show();
            } else {
            }

        } catch (error) {
            console.warn('[StatusBar] Update failed:', error);
            this.statusBarItem.text = `$(warning) Flink Offline`;
            this.statusBarItem.tooltip = 'Could not connect to Flink JobManager';
        }
    }

    dispose() {
        this.statusBarItem.dispose();
        if (this.timer) {
            clearInterval(this.timer);
        }
    }
}
