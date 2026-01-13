// The module 'vscode' contains the VS Code extensibility API
import * as vscode from 'vscode';
import { Logger } from './utils/logger';
import { WebviewHelper } from './utils/webviewHelper';
import { FlinkSqlNotebookSerializer } from './notebookSerializer';
import { FlinkNotebookController } from './notebookController';
import { FlinkJobsProvider } from './jobsProvider';
import { FlinkTaskManagersProvider } from './taskManagersProvider';
import { FlinkGatewayClient } from './flinkClient';
import { SessionManager } from './sessionManager';
import { FlinkCatalogProvider } from './catalogProvider';
import { FlinkStatusBar } from './statusBar';
import { FlinkSqlCompletionItemProvider } from './completionProvider';
import { FlinkObjectDetailsProvider } from './objectDetailsProvider';
import { FlinkConnectionsProvider, ConnectionTreeItem } from './connectionsProvider';
import { FlinkSessionsProvider, SessionTreeItem } from './sessionsProvider';
import { ConnectionManager } from './connectionManager';

// This method is called when your extension is activated
export function activate(context: vscode.ExtensionContext) {

	// Initialize Logger
	Logger.initialize('Apache Flink');
	Logger.info('Congratulations, your extension "apache-flink-vscode-extension" is now active!');

	// Initialize Connection Manager (handles saved connections)
	const connectionManager = new ConnectionManager(context);
	context.subscriptions.push(connectionManager);

	// Notebook Serializer
	const notebookSerializer = vscode.workspace.registerNotebookSerializer(
		'flink-sql-notebook',
		new FlinkSqlNotebookSerializer()
	);

	// Session Manager (sessions are linked to connections)
	const sessionManager = new SessionManager(context, connectionManager);

	// Helper to get client for active session
	const getActiveClient = (): FlinkGatewayClient => {
		const activeHandle = sessionManager.getCurrentSessionHandle();
		if (activeHandle) {
			const client = sessionManager.getClientForSession(activeHandle);
			if (client) { return client; }
		}
		// Fallback to first connection
		const conn = connectionManager.getFirstConnection();
		if (conn) {
			return new FlinkGatewayClient(conn.gatewayUrl, conn.jobManagerUrl);
		}
		return new FlinkGatewayClient('http://localhost:8083', 'http://localhost:8081');
	};

	// Status Bar (uses active session's connection)
	const statusBar = new FlinkStatusBar(getActiveClient());
	context.subscriptions.push(statusBar);

	// Update status bar when session changes
	sessionManager.onDidChangeActiveSession(() => {
		statusBar.updateClient(getActiveClient());
		statusBar.update();
	});

	// Track all notebook controllers (one per session)
	const controllers: Map<string, FlinkNotebookController> = new Map();

	// Function to create controllers for all sessions
	const createControllersForSessions = () => {
		const sessions = sessionManager.getAllSessions();

		// Create controllers for new sessions
		for (const session of sessions) {
			if (!controllers.has(session.handle)) {
				const controller = new FlinkNotebookController(context, connectionManager, sessionManager, session);
				controllers.set(session.handle, controller);
				context.subscriptions.push(controller);

				// Wire up refresh on execute
				controller.onDidExecute(() => {
					runningJobsProvider.refresh();
					historyJobsProvider.refresh();
					catalogProvider.refresh();
					tmsProvider.refresh();
					statusBar.update();
					connectionsProvider.refresh();
					sessionsProvider.refresh();
				});

				Logger.info(`[Extension] Created controller for session: ${session.name}`);
			}
		}

		// Remove controllers for deleted sessions
		const sessionHandles = new Set(sessions.map(s => s.handle));
		for (const [handle, controller] of controllers.entries()) {
			if (!sessionHandles.has(handle)) {
				controller.dispose();
				controllers.delete(handle);
				Logger.info(`[Extension] Disposed controller for deleted session`);
			}
		}
	};

	// Initial controller creation
	createControllersForSessions();

	// Listen for session changes to create/dispose controllers
	sessionManager.onDidChangeSessions(() => {
		createControllersForSessions();
	});

	// Get URLs for providers (use first connection or defaults)
	const getDefaultUrls = () => {
		const conn = connectionManager.getFirstConnection();
		return {
			gatewayUrl: conn?.gatewayUrl || 'http://localhost:8083',
			jobManagerUrl: conn?.jobManagerUrl || 'http://localhost:8081'
		};
	};

	const { gatewayUrl, jobManagerUrl } = getDefaultUrls();

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
	const tmsProvider = new FlinkTaskManagersProvider(gatewayUrl, jobManagerUrl, sessionManager);
	vscode.window.registerWebviewViewProvider('flinkTaskManagers', tmsProvider);

	// Register Explorer sidebar
	const catalogProvider = new FlinkCatalogProvider(context, gatewayUrl, jobManagerUrl, sessionManager);
	vscode.window.registerTreeDataProvider('flinkExplorer', catalogProvider);

	const objectDetailsProvider = new FlinkObjectDetailsProvider(getActiveClient(), sessionManager);
	vscode.window.registerWebviewViewProvider('flinkObjectDetails', objectDetailsProvider);

	// Update providers when active session changes (different connection)
	sessionManager.onDidChangeActiveSession((handle) => {
		const conn = sessionManager.getConnectionForSession(handle);
		if (conn) {
			const client = new FlinkGatewayClient(conn.gatewayUrl, conn.jobManagerUrl);
			runningJobsProvider.updateConnection(conn.gatewayUrl, conn.jobManagerUrl);
			historyJobsProvider.updateConnection(conn.gatewayUrl, conn.jobManagerUrl);
			tmsProvider.updateConnection(conn.gatewayUrl, conn.jobManagerUrl);
			catalogProvider.updateConnection(conn.gatewayUrl, conn.jobManagerUrl);
			objectDetailsProvider.updateClient(client);
			sqlCompletionProvider.updateClient(client);
		}
	});

	// Register Connections sidebar
	const connectionsProvider = new FlinkConnectionsProvider(connectionManager);
	const connectionsTreeView = vscode.window.createTreeView('flinkConnections', {
		treeDataProvider: connectionsProvider
	});

	// Register Sessions sidebar
	const sessionsProvider = new FlinkSessionsProvider(sessionManager, connectionManager);
	const sessionsTreeView = vscode.window.createTreeView('flinkSessions', {
		treeDataProvider: sessionsProvider
	});

	const refreshExplorerCommand = vscode.commands.registerCommand('flinkExplorer.refresh', () => {
		catalogProvider.refresh();
	});

	const selectDatabaseCommand = vscode.commands.registerCommand('flinkExplorer.selectDatabase', (item) => {
		catalogProvider.selectDatabase(item);
	});

	const selectCatalogCommand = vscode.commands.registerCommand('flinkExplorer.selectCatalog', () => {
		catalogProvider.selectCatalog();
	});

	const selectObjectCommand = vscode.commands.registerCommand('flinkExplorer.selectObject', async (catalog, database, object, type) => {
		await vscode.commands.executeCommand('flinkObjectDetails.focus');
		objectDetailsProvider.update(catalog, database, object, type);
	});

	const useDatabaseCommand = vscode.commands.registerCommand('flinkExplorer.useDatabase', (item: any) => {
		catalogProvider.selectDatabase(item);
	});

	const refreshDatabaseCommand = vscode.commands.registerCommand('flinkExplorer.refreshDatabase', (item: any) => {
		catalogProvider.refreshDatabase(item);
	});

	const refreshCatalogCommand = vscode.commands.registerCommand('flinkExplorer.refreshCatalog', (item: any) => {
		catalogProvider.refreshCatalog(item);
	});

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

	// Connection management commands
	const refreshConnectionsCommand = vscode.commands.registerCommand('flinkConnections.refresh', () => {
		connectionsProvider.refresh();
	});

	const addConnectionCommand = vscode.commands.registerCommand('flinkConnections.add', async () => {
		await connectionManager.promptAddConnection();
	});

	const editConnectionCommand = vscode.commands.registerCommand('flinkConnections.edit', async (item: ConnectionTreeItem) => {
		if (item?.connection) {
			await connectionManager.promptEditConnection(item.connection.id);
		}
	});

	const removeConnectionCommand = vscode.commands.registerCommand('flinkConnections.remove', async (item: ConnectionTreeItem) => {
		if (item?.connection) {
			await connectionManager.promptRemoveConnection(item.connection.id);
		}
	});

	// Session management commands
	const refreshSessionsCommand = vscode.commands.registerCommand('flinkSessions.refresh', () => {
		sessionsProvider.refresh();
	});

	const createSessionTreeCommand = vscode.commands.registerCommand('flinkSessions.create', async () => {
		await sessionManager.createSession();
	});

	const setActiveSessionCommand = vscode.commands.registerCommand('flinkSessions.setActive', async (item: SessionTreeItem) => {
		if (item?.session) {
			await sessionManager.setActiveSession(item.session.handle);
			vscode.window.showInformationMessage(`Switched to session: ${item.session.name}`);
		}
	});

	const removeSessionCommand = vscode.commands.registerCommand('flinkSessions.remove', async (item: SessionTreeItem) => {
		if (item?.session) {
			await sessionManager.removeSession(item.session.handle);
		}
	});

	// Legacy configure command (for backwards compatibility)
	const configureCommand = vscode.commands.registerCommand('flink.configureConnection', async () => {
		// Open the add connection dialog
		await connectionManager.promptAddConnection();
	});

	// SQL Completion Provider
	const sqlCompletionProvider = new FlinkSqlCompletionItemProvider(getActiveClient(), sessionManager);
	const completionDisposable = vscode.languages.registerCompletionItemProvider('apache-flink', sqlCompletionProvider, '.', ' ');

	context.subscriptions.push(
		notebookSerializer,
		runningJobsTreeView,
		historyJobsTreeView,
		connectionsTreeView,
		sessionsTreeView,
		refreshRunningCommand,
		refreshHistoryCommand,
		cancelJobCommand,
		refreshTMCommand,
		refreshConnectionsCommand,
		addConnectionCommand,
		editConnectionCommand,
		removeConnectionCommand,
		refreshSessionsCommand,
		createSessionTreeCommand,
		setActiveSessionCommand,
		removeSessionCommand,
		refreshExplorerCommand,
		selectDatabaseCommand,
		selectCatalogCommand,
		useDatabaseCommand,
		refreshDatabaseCommand,
		refreshCatalogCommand,
		configureCommand,
		createSessionCommand,
		selectSessionCommand,
		sessionManager,
		completionDisposable
	);

	// Command: Show Job Detail
	context.subscriptions.push(vscode.commands.registerCommand('flink.showJobDetail', async (jobId: string, status?: string) => {
		if (!jobId) { return; }

		let jobStatus = status;
		const client = getActiveClient();
		if (!jobStatus) {
			try {
				const details = await client.getJobDetails(jobId);
				if (details) {
					jobStatus = details.state;
				}
			} catch (e) {
				Logger.warn(`Failed to fetch status for job ${jobId}: ${e}`);
			}
		}

		const safeStatus = (jobStatus || 'running').toLowerCase();

		// Get JobManager URL from active session's connection
		const activeHandle = sessionManager.getCurrentSessionHandle();
		let jmUrl = 'http://localhost:8081';
		if (activeHandle) {
			const conn = sessionManager.getConnectionForSession(activeHandle);
			if (conn) { jmUrl = conn.jobManagerUrl; }
		}

		const cleanBaseUrl = jmUrl.replace(/\/$/, '');
		const url = `${cleanBaseUrl}/#/job/${safeStatus}/${jobId}/overview`;

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

		panel.webview.html = WebviewHelper.getFrameHtml(url);
	}));
}

// This method is called when your extension is deactivated
export function deactivate() { }
