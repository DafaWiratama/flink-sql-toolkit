# Apache Flink SQL Notebook & Toolkit for VS Code



Transform your editor into a powerful **Stream Processing** and **Big Data** workstation. Write, debug, and execute Flink SQL queries in real-time. Manage your Flink clusters, explore catalogs, and monitor jobs without leaving VS Code.

Designed for **Data Engineers**, **Platform Engineers**, and **Data Scientists** working with **Apache Flink**, **Kafka**, and **Real-time Analytics**.

---

## ✨ Why this Extension? (Key Features)

- **📓 Interactive Notebooks**:
  - Run Flink SQL interactively in `.fsqlnb` or `.flinksql` files.
  - **Live Streaming Results**: Watch your streaming data flow in real-time within the editor.
  - **Sorting & Filtering**: Client-side analysis of result sets. Export to CSV/JSON.

- **⚡ Intelligent Coding**:
  - **Context-Aware Autocomplete**: Smart suggestions for Tables, Views, Columns, and Functions.
  - **Snippet Library**: Built-in templates for Kafka Connectors, DataGen, Window TVFs (`TUMBLE`, `HOP`), and more.
  - **Syntax Highlighting**: Dedicated support for Flink SQL dialect.

- **🔍 Cluster Explorer**:
  - **Metadata Browser**: Navigate Catalogs, Databases, Tables, and Views.
  - **One-Click Actions**: Right-click to "Script SELECT" or "Copy CREATE TABLE" statements.
  - **View Schema**: Instantly check column types and table properties.

- **🛡️ Job & System Management**:
  - **Job Monitoring**: View Running and Completed jobs.
  - **Control**: Cancel jobs directly from the sidebar.
  - **Deep Dive**: Open the Flink Dashboard for specific jobs with a single click.
  - **System Health**: Monitor TaskManagers, Slots, and resource usage.

---

## 🚀 Zero to Hero: Quick Start Guide

 Follow these steps to run your first Flink SQL streaming job in minutes.

### 1. Prerequisites
- **VS Code**: v1.85+
- **Apache Flink Cluster**: Running (v1.16+ recommended).
- **Flink SQL Gateway**: Must be running (default port `8083`).

### 2. Setup
1.  Install **Flink SQL Toolkit** from the VS Code Marketplace.
2.  Open VS Code and navigate to the **Flink Activity Bar** (Flink Logo).
3.  Click **"Configure Connection"** (or run command `Flink: Configure Connection`).
    -   **Gateway URL**: `http://localhost:8083`
    -   **JobManager URL**: `http://localhost:8081`

### 3. "Hello World" Streaming Job
Create a new file named `demo.fsqlnb` or `demo.flinksql` and paste the following:

```sql
-- 1. Create a Source Table (Data Generator)
CREATE TABLE orders (
    order_id BIGINT,
    price DECIMAL(10, 2),
    buyer STRING,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

-- 2. Create a Sink Table (Print to Console/Log)
CREATE TABLE print_sink (
    order_id BIGINT,
    price DECIMAL(10, 2),
    buyer STRING
) WITH (
    'connector' = 'print'
);

-- 3. Run a Continuous Query
INSERT INTO print_sink
SELECT order_id, price, buyer
FROM orders
WHERE price > 10;
```

**Run it!** Click the "Run" lens (or `Ctrl+Enter`). You will see the job submitted and results streaming (for SELECT queries) or job ID returned (for INSERT).

---

## ⚙️ Configuration

You can customize the extension via `.vscode/settings.json`:

| Setting | Default | Description |
| :--- | :--- | :--- |
| `flink.gatewayUrl` | `http://localhost:8083` | Flink SQL Gateway REST Endpoint. |
| `flink.jobManagerUrl` | `http://localhost:8081` | Flink JobManager Dashboard URL. |
| `flink.sessionName` | `default` | Default session name for the gateway. |

---

## 💡 Pro Tips

*   **Invalid Session?** No problem. The extension automatically recovers invalid sessions by creating a new `default` session, so you never lose your flow.
*   **Column Autocomplete**: Type `SELECT t.` to see column suggestions for table aliased as `t`.
*   **Drag & Drop**: Drag a table from the Explorer into your editor to insert its full name.

---

## 🔧 Troubleshooting

**"Session is invalid" loop?**
- Ensure your Flink SQL Gateway is running and accessible.
- Check logs: `View > Output > Flink SQL Toolkit`.

**"Failed to fetch metadata"?**
- Verify the `gatewayUrl` in settings.
- Ensure Flink is listening on the correct host/port (docker containers might need host networking or port mapping).

---

## 🤝 Contributing & Support

Found a bug? Want a feature?
[Open an Issue on GitHub](https://github.com/DafaWiratama/flink-sql-toolkit/issues)

**License**: MIT
