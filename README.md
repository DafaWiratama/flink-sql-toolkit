
# Apache Flink SQL Notebook for VS Code

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Apache Flink SQL Notebook** transforms VS Code into a first-class Interactive Development Environment (IDE) for Apache Flink. Write SQL, stream results in real-time, explore cluster metadata, and manage jobs—all without leaving your editor.

---

## ✨ Key Enhancements

Recent updates allow you to do more with your data:

- 📊 **Advanced Visualization**: Client-side **Sorting** and **Filtering** on query results, plus **CSV/JSON Export**.
- 🧠 **Intelligent Completion**: Context-aware **Column Auto-Completion** that understands table aliases and joins.
- ⚡ **Productivity Snippets**: Built-in templates for Kafka tables, Window TVFs, and DataGen.
- 🛠️ **Interactive Explorer**: Click to **Script SELECT** queries or **Copy DDL** statements instantly.

---

## 🚀 Features

### 📓 Interactive Notebooks
- **`.flinksql` Support**: Create notebook files to organize your queries.
- **Streaming Results**: Run queries and watch results stream in real-time.
- **Pause & Resume**: Control the flow of streaming data.
- **Result Grid**:
    - **Sort**: Click headers to sort by column.
    - **Filter**: Type to filter rows instantly.
    - **Export**: Download results as CSV or copy as JSON.

### 🔍 Cluster Explorer
Navigate your Flink ecosystem from the sidebar:
- **Metadata Browser**: Drill down into Catalogs, Databases, and Tables.
- **Quick Actions**:
    - Right-click or use the **Details View** to interact with objects.
    - **Script SELECT**: Inserts a `SELECT * ... LIMIT 100` snippet.
    - **Copy DDL**: Copies the `CREATE TABLE` statement to your clipboard.

### 🛡️ System Monitoring
- **Running Jobs**: View active jobs, their status, and duration.
- **Job History**: Access past job executions.
- **One-Click Cancel**: Stop running jobs directly from the sidebar.
- **Task Manager Details**: Monitor cluster health, slots, and resource usage.

### ⚡ Developer Experience
- **Auto-Completion**:
    - Keywords, Functions, Catalogs, Databases, Tables.
    - **Smart Column Suggestions**: Detects tables in `FROM/JOIN` clauses (including aliases like `t.col`) to suggest relevant columns.
- **Code Snippets**: Type `flink-` to access templates:
    - `flink-create-kafka`: Kafka Source/Sink table.
    - `flink-create-print`: Print sink.
    - `flink-window-tumble` / `flink-window-hop`: Window aggregations.

### 🔌 Connectivity
- **Auto-Configuration**: Prompts to configure connection on first use.
- **Session Management**: Create and switch between Flink sessions easily.

---

## 🛠️ Getting Started

### Prerequisites
1.  **VS Code**: Version `1.85.0` or higher.
2.  **Apache Flink Cluster**: A running Flink cluster.
3.  **Flink SQL Gateway**: Must be running and accessible (default port `8083`).

### Installation
1.  Install the extension from the Marketplace.
2.  Open or create a `.flinksql` file.
3.  **Configure Connection**: Click the "Configure Connection" icon in the **Flink Explorer** title bar or use the Command Palette.

### Configuration
Update these settings in `.vscode/settings.json` or Global Settings:

| Setting | Default | Description |
| :--- | :--- | :--- |
| `flink.gatewayUrl` | `http://localhost:8083` | URL of the Flink SQL Gateway REST API. |
| `flink.jobManagerUrl` | `http://localhost:8081` | URL of the Flink JobManager Dashboard (for monitoring). |
| `flink.sessionName` | `default` | Name of the Flink Session to use. |

---

## ⌨️ Useful Commands

| Command | Description |
| :--- | :--- |
| `Flink: Refresh` | Refreshes Explorer and Job lists. |
| `Flink: Configure Connection` | Update Gateway/JobManager URLs. |
| `Flink: Show Job Details` | Open Flink Dashboard for a job. |
| `Flink: Stop Job` | Cancel a running job. |
| `Flink: Create Session` | Create a new session on the Gateway. |

## 📝 License

This project is licensed under the [MIT License](LICENSE.md).
