// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import { Logger } from './utils/logger';
import { FlinkSqlNotebookSerializer } from './notebookSerializer';
import { FlinkNotebookController } from './notebookController';
import { FlinkJobsProvider } from './jobsProvider';
import { FlinkTaskManagersProvider } from './taskManagersProvider';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { FlinkCatalogProvider } from './catalogProvider';
import { FlinkStatusBar } from './statusBar';
import { FlinkSqlCompletionItemProvider } from './completionProvider';

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {

	// Initialize Logger
	Logger.initialize('Apache Flink');

	// Use the console to output diagnostic information (console.log) and errors (console.error)
	// This line of code will only be executed once when your extension is activated
	Logger.info('Congratulations, your extension "apache-flink-vscode-extension" is now active!');

	// The command has been defined in the package.json file
	// Now provide the implementation of the command with registerCommand
	// The commandId parameter must match the command field in package.json
	const disposable = vscode.commands.registerCommand('apache-flink-vscode-extension.helloWorld', () => {
		// The code you place here will be executed every time your command is executed
		// Display a message box to the user
		vscode.window.showInformationMessage('Hello World from apache-flink-vscode-extension!');
	});

	const notebookSerializer = vscode.workspace.registerNotebookSerializer(
		'flink-sql-notebook',
		new FlinkSqlNotebookSerializer()
	);

	const config = vscode.workspace.getConfiguration('flink');
	const gatewayUrl = config.get<string>('gatewayUrl', 'http://localhost:8083');
	const jobManagerUrl = config.get<string>('jobManagerUrl', 'http://localhost:8081');

	const client = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
	const sessionManager = new SessionManager(context, client);

	const controller = new FlinkNotebookController(context, sessionManager);

	const statusBar = new FlinkStatusBar(client);
	context.subscriptions.push(statusBar);

	// Intelligent Refresh
	controller.onDidExecute(() => {
		runningJobsProvider.refresh();
		historyJobsProvider.refresh();
		catalogProvider.refresh();
		tmsProvider.refresh();
		statusBar.update();
	});

	// Register Flink Jobs sidebar

	const runningJobsProvider = new FlinkJobsProvider(gatewayUrl, jobManagerUrl, 'RUNNING');
	const runningJobsTreeView = vscode.window.createTreeView('flinkRunningJobs', {
		treeDataProvider: runningJobsProvider
	});

	const historyJobsProvider = new FlinkJobsProvider(gatewayUrl, jobManagerUrl, 'HISTORY');
	const historyJobsTreeView = vscode.window.createTreeView('flinkJobHistory', {
		treeDataProvider: historyJobsProvider
	});

	// Register refresh commands
	const refreshRunningCommand = vscode.commands.registerCommand('flinkRunningJobs.refresh', () => {
		runningJobsProvider.refresh();
	});
	const refreshHistoryCommand = vscode.commands.registerCommand('flinkJobHistory.refresh', () => {
		historyJobsProvider.refresh();
	});

	// Register TaskManagers sidebar
	const tmsProvider = new FlinkTaskManagersProvider(gatewayUrl, jobManagerUrl);
	vscode.window.registerWebviewViewProvider('flinkTaskManagers', tmsProvider);

	// Register Explorer sidebar
	const catalogProvider = new FlinkCatalogProvider(context, gatewayUrl, jobManagerUrl, sessionManager);
	vscode.window.registerTreeDataProvider('flinkExplorer', catalogProvider);

	const refreshExplorerCommand = vscode.commands.registerCommand('flinkExplorer.refresh', () => {
		catalogProvider.refresh();
	});

	const selectDatabaseCommand = vscode.commands.registerCommand('flinkExplorer.selectDatabase', (item) => {
		catalogProvider.selectDatabase(item);
	});

	// Register cancel command
	// Register cancel command
	const cancelJobCommand = vscode.commands.registerCommand('flinkJobs.cancel', (item: any) => {
		runningJobsProvider.cancelJob(item);
	});

	// Register Session commands
	const createSessionCommand = vscode.commands.registerCommand('flink.createSession', () => {
		sessionManager.createSession();
	});
	const selectSessionCommand = vscode.commands.registerCommand('flink.selectSession', () => {
		sessionManager.pickSession();
	});

	const refreshTMCommand = vscode.commands.registerCommand('flinkTaskManagers.refresh', () => {
		tmsProvider.refresh();
	});

	const configureCommand = vscode.commands.registerCommand('flink.configureConnection', async () => {
		const config = vscode.workspace.getConfiguration('flink');
		const currentGateway = config.get<string>('gatewayUrl', 'http://localhost:8083');
		const currentJobManager = config.get<string>('jobManagerUrl', 'http://localhost:8081');

		const gatewayUrl = await vscode.window.showInputBox({
			title: 'Flink SQL Gateway URL',
			prompt: 'Enter the URL of the Flink SQL Gateway',
			value: currentGateway,
			ignoreFocusOut: true
		});
		if (gatewayUrl === undefined) { return; }

		const jobManagerUrl = await vscode.window.showInputBox({
			title: 'Flink JobManager URL',
			prompt: 'Enter the URL of the Flink JobManager (Dashboard)',
			value: currentJobManager,
			ignoreFocusOut: true
		});
		if (jobManagerUrl === undefined) { return; }

		const currentSessionName = config.get<string>('sessionName', 'default');
		const sessionName = await vscode.window.showInputBox({
			title: 'Default Session Name',
			prompt: 'Enter the name for the default Flink Session',
			value: currentSessionName,
			ignoreFocusOut: true
		});
		if (sessionName === undefined) { return; }

		await config.update('gatewayUrl', gatewayUrl, vscode.ConfigurationTarget.Global);
		await config.update('jobManagerUrl', jobManagerUrl, vscode.ConfigurationTarget.Global);
		await config.update('sessionName', sessionName, vscode.ConfigurationTarget.Global);

		runningJobsProvider.updateConnection(gatewayUrl, jobManagerUrl);
		historyJobsProvider.updateConnection(gatewayUrl, jobManagerUrl);
		tmsProvider.updateConnection(gatewayUrl, jobManagerUrl);
		catalogProvider.updateConnection(gatewayUrl, jobManagerUrl);

		const newClient = new FlinkGatewayClient(gatewayUrl, jobManagerUrl);
		sessionManager.updateClient(newClient);
		sqlCompletionProvider.updateClient(newClient);

		controller.resetConnection();

		vscode.window.showInformationMessage('Flink connection settings updated successfully.');
	});

	const sqlCompletionProvider = new FlinkSqlCompletionItemProvider(client, sessionManager);
	const completionDisposable = vscode.languages.registerCompletionItemProvider('apache-flink', sqlCompletionProvider, '.', ' '); // Trigger on dot and space

	context.subscriptions.push(disposable, notebookSerializer, controller, runningJobsTreeView, historyJobsTreeView, refreshRunningCommand, refreshHistoryCommand, cancelJobCommand, refreshTMCommand, refreshExplorerCommand, selectDatabaseCommand, configureCommand, createSessionCommand, selectSessionCommand, sessionManager, completionDisposable);

	// Command: Show Job Detail
	context.subscriptions.push(vscode.commands.registerCommand('flink.showJobDetail', async (jobId: string, status?: string) => {
		if (!jobId) { return; }

		let jobStatus = status;
		if (!jobStatus) {
			// Fallback: Fetch details if status not passed
			try {
				const details = await client.getJobDetails(jobId);
				if (details) {
					jobStatus = details.state;
				}
			} catch (e) {
				Logger.warn(`Failed to fetch status for job ${jobId}: ${e}`);
			}
		}

		// Default to 'running' if still unknown, though likely won't happen if job exists
		const safeStatus = (jobStatus || 'running').toLowerCase();

		const config = vscode.workspace.getConfiguration('flink');
		const jobManagerUrl = config.get<string>('jobManagerUrl', 'http://localhost:8081');

		// Clean up the URL to avoid double slashes
		const cleanBaseUrl = jobManagerUrl.replace(/\/$/, '');
		// User requested format: /#/job/running/<id>/overview
		const url = `${cleanBaseUrl}/#/job/${safeStatus}/${jobId}/overview`;

		// Create Webview Panel
		const panel = vscode.window.createWebviewPanel(
			'flinkJobDetail',
			`Job: ${jobId}`,
			vscode.ViewColumn.One,
			{
				enableScripts: true,
				retainContextWhenHidden: true,
				localResourceRoots: []
			}
		);

		// Allow localhost content in Webview
		panel.webview.html = `
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <style>
                    html, body { height: 100%; width: 100%; margin: 0; padding: 0; overflow: hidden; }
                    iframe { width: 100%; height: 100%; border: none; }
                </style>
            </head>
            <body>
                <iframe src="${url}"></iframe>
            </body>
            </html>
        `;
	}));
}

// This method is called when your extension is deactivated
export function deactivate() { }
