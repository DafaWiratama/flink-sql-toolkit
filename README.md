# Apache Flink SQL Notebook for VS Code

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Apache Flink SQL Notebook** is a powerful VS Code extension that transforms your editor into a fully-featured interactive development environment for Apache Flink SQL. Connect to your Flink cluster, write SQL queries in notebooks, visualize results in real-time, and manage your jobs—all without leaving VS Code.

---

## ✨ Features

### 📓 Interactive SQL Notebooks
- **Rich Notebook Interface**: Create `.flinksql` files to write and execute SQL queries cell-by-cell.
- **Syntax Highlighting**: Full syntax support for Flink SQL dialect.
- **Streaming Results**: Visualize streaming query results in real-time with dynamic tables that update as new data arrives.

### 🔍 comprehensive Explorer
- **Metadata Browser**: Explore Catalogs, Databases, Tables, and Views directly from the sidebar.
- **Job Management**:
    - **Running Jobs**: Monitor active jobs, view status, and cancel them with a single click.
    - **Job History**: Access details of completed or failed jobs.
    - **Deep Dive**: Click on any job to instantly open the full Flink Web Dashboard for detailed metrics and logs.

### 🛠️ System Monitoring
- **Cluster Status**: View real-time cluster health, including TaskManager count, available slots, and resource usage.
- **Task Manager Details**: Drill down into individual TaskManagers to see hardware specs and slot distribution.

### ⚡ Developer Experience
- **Intelligent Refresh**: The UI automatically updates metadata and job lists when you execute modification statements (like `CREATE TABLE` or `DROP VIEW`).
- **Error Handling**: Friendly error messages help you quickly identify issues like resource constraints or syntax errors.

---

## 🚀 Getting Started

### Prerequisites

1.  **VS Code**: Version 1.82.0 or higher.
2.  **Apache Flink Cluster**: A running Flink cluster (Session or Application mode).
3.  **Flink SQL Gateway**: The SQL Gateway must be running and accessible.

### Installation

1.  Install the extension from the VS Code Marketplace.
2.  Open or create a file with the `.flinksql` extension.
3.  The extension will activate and attempt to connect to the default local Flink endpoints.

---

## ⚙️ Configuration

You can configure the connection details in your VS Code settings (`settings.json`).

| Setting | Default | Description |
| :--- | :--- | :--- |
| `flink.gatewayUrl` | `http://localhost:8083` | The URL of the **Flink SQL Gateway** REST API. Ensure the gateway is started and this port is accessible. |
| `flink.jobManagerUrl` | `http://localhost:8081` | The URL of the **Flink JobManager** Web Dashboard. Used for status checks and linking to job details. |

> **Tip:** You can also configure these settings by clicking the "Configure Connection" button in the Flink Explorer title bar.

---

## ⌨️ Commands

Access these commands from the Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P`) or via the UI buttons:

- **Flink: Refresh**: Refreshes the Explorer view and Job lists.
- **Flink: Configure Connection**: open settings to update Gateway and JobManager URLs.
- **Flink: Show Job Details**: Open the Flink Web Dashboard for a specific job.
- **Flink: Stop Job**: Cancel a running job directly from the VS Code sidebar.

---

## 🔧 Known Issues

- Complex split of SQL statements might encounter issues if semicolons are used within string literals. A proper SQL parser is planned for future updates.
- Metadata operations currently rely on the SQL Gateway's compatibility. Ensure your Gateway version supports standard REST endpoints.

---

## 📝 License

This project is licensed under the [MIT License](LICENSE.md).

For more information, visit the [GitHub Repository](https://github.com/DafaWiratama/apache-flink-sql-workspace-vsix).
