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

    startPolling() {
        this.timer = setInterval(() => this.update(), 10000); // Poll every 10s
    }

    async update() {
        try {
            const tms = await this.client.getTaskManagers();

            let totalSlots = 0;
            let freeSlots = 0;

            for (const tm of tms) {
                totalSlots += (tm.slotsNumber || 0);
                freeSlots += (tm.freeSlots || 0);
            }

            const activeSlots = totalSlots - freeSlots;

            // Update Status Bar
            // Icon: server or pulse
            this.statusBarItem.text = `$(server) ${activeSlots}/${totalSlots} Slots`;
            this.statusBarItem.tooltip = `Flink Cluster Status\n\nActive Tasks: ${activeSlots}\nFree Slots: ${freeSlots}\nTotal Slots: ${totalSlots}`;

            if (totalSlots > 0) {
                this.statusBarItem.show();
            } else {
                this.statusBarItem.hide(); // Hide if no cluster connection
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
