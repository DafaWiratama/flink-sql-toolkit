import * as vscode from 'vscode';

export class Logger {
    private static _outputChannel: vscode.OutputChannel;

    public static initialize(channelName: string) {
        this._outputChannel = vscode.window.createOutputChannel(channelName);
    }

    public static info(message: string, ...args: any[]): void {
        this._log('INFO', message, args);
    }

    public static error(message: string, ...args: any[]): void {
        this._log('ERROR', message, args);
    }

    public static warn(message: string, ...args: any[]): void {
        this._log('WARN', message, args);
    }

    private static _log(level: string, message: string, args: any[]): void {
        if (!this._outputChannel) {
            // Fallback if not initialized (though it should be)
            console.log(`[${level}] ${message}`, ...args);
            return;
        }

        const timestamp = new Date().toLocaleTimeString();
        let formattedMessage = `[${timestamp}] [${level}] ${message}`;

        if (args && args.length > 0) {
            args.forEach(arg => {
                if (typeof arg === 'object') {
                    formattedMessage += ' ' + JSON.stringify(arg);
                } else {
                    formattedMessage += ' ' + arg;
                }
            });
        }

        this._outputChannel.appendLine(formattedMessage);
    }

    public static show(): void {
        this._outputChannel?.show(true);
    }

    public static dispose(): void {
        this._outputChannel?.dispose();
    }
}
