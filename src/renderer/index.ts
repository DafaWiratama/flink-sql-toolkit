
import type { ActivationFunction } from 'vscode-notebook-renderer';

// State to persist per output element
interface RenderState {
    pageSize: number;
    currentPage: number;
    autoScroll: boolean;
    isPaused: boolean;
    latestJson: any;
}

const stateMap = new WeakMap<any, RenderState>();

// Helper to render the view based on state
function render(element: any) {
    const state = stateMap.get(element);
    if (!state || !state.latestJson) {return;}

    const json = state.latestJson;
    const rows = json.data || [];
    const columns = json.schema.fields || [];
    const meta = json.metadata || {};
    const globalOffset = typeof meta.offset === 'number' ? meta.offset : 0;

    // Auto-scroll logic (only if not paused and autoScroll enabled)
    const totalRows = rows.length;
    const totalPages = Math.ceil(totalRows / state.pageSize) || 1;

    if (state.autoScroll && !state.isPaused) {
        state.currentPage = totalPages;
    } else {
        if (state.currentPage > totalPages) {state.currentPage = totalPages;}
    }

    // Slicing
    const startIdx = (state.currentPage - 1) * state.pageSize;
    const endIdx = startIdx + state.pageSize;
    const pageRows = rows.slice(startIdx, endIdx);

    // CSS
    const styles = `
                <style>
                    .dataframe { border-collapse: separate; border-spacing: 0; width: 100%; font-family: var(--vscode-editor-font-family); font-size: 11px; }
                    .dataframe td, .dataframe th { padding: 8px; text-align: right; border-right: 1px solid var(--vscode-textBlockQuote-border); border-bottom: 1px solid var(--vscode-textBlockQuote-border); }
                    .dataframe th { 
                        background: var(--vscode-editor-background); 
                        position: sticky; 
                        top: 0; 
                        z-index: 10; 
                        border-bottom: 2px solid var(--vscode-editorGroup-border);
                        box-shadow: 0 2px 5px var(--vscode-widget-shadow, rgba(0,0,0,0.1)); /* Shadow for depth */
                    }
                    .dataframe th:first-child, .dataframe td:first-child { 
                        position: sticky; 
                        left: 0; 
                        border-right: 2px solid var(--vscode-editorGroup-border); /* Thicker separation for index */
                        background: var(--vscode-editor-background); 
                        font-weight: bold; 
                        z-index: 11;
                    }
                    /* Ensure corner (Top-Left) is highest z-index */
                    .dataframe th:first-child { z-index: 12; }
                    
                    .dataframe tr:nth-child(even) td { background: var(--vscode-notebook-cellEditorBackground); }
                    .dataframe tr:hover td { background: var(--vscode-list-hoverBackground); }
                    
                    .toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px; font-size: 11px; font-family: var(--vscode-font-family); padding: 4px; background: var(--vscode-editor-background); position: sticky; top: 0; left: 0; border-bottom: 1px solid var(--vscode-widget-border); }
                    .btn-group button { background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; padding: 4px 8px; cursor: pointer; margin-right: 2px; }
                    .btn-group button:hover { background: var(--vscode-button-secondaryHoverBackground); }
                    .select-pagesize { background: var(--vscode-dropdown-background); color: var(--vscode-dropdown-foreground); border: 1px solid var(--vscode-dropdown-border); }
                    .info { color: var(--vscode-descriptionForeground); margin-left: 10px; font-size: 10px; }
                    .badge { padding: 2px 6px; border-radius: 3px; font-weight: bold; font-size: 10px; margin-right: 10px; }
                    .badge-live { background-color: var(--vscode-button-background); color: white; }
                    .badge-finished { background-color: var(--vscode-testing-iconPassed); color: white; }
                    .badge-paused { background-color: var(--vscode-inputValidation-warningBackground); color: var(--vscode-inputValidation-warningForeground); border: 1px solid var(--vscode-inputValidation-warningBorder); }
                </style>
                `;

    // Header
    const headerCells = columns.map((c: any) => `<th>${c.name}</th>`).join('');
    const headerRow = `<tr><th>#</th>${headerCells}</tr>`;

    // Body
    const body = pageRows.map((row: any, i: number) => {
        const globalIndex = globalOffset + startIdx + i + 1;
        let cellData: any[] = [];
        if (row.fields && Array.isArray(row.fields)) {cellData = row.fields;}
        else if (Array.isArray(row)) {cellData = row;}
        else {cellData = columns.map((c: any) => row[c.name]);}

        const cells = cellData.map((v: any) => `<td>${v === null || v === undefined ? '' : String(v)}</td>`).join('');
        return `<tr><td>${globalIndex}</td>${cells}</tr>`;
    }).join('');

    // Toolbar Elements
    const pSize = state.pageSize;
    const cPage = state.currentPage;
    const tPages = totalPages;

    // Status Logic
    let statusBadge = '';
    if (meta.isStreaming) {
        if (meta.isComplete) {
            statusBadge = '<span class="badge badge-finished">● FINISHED</span>';
        } else {
            statusBadge = '<span class="badge badge-live">● LIVE</span>';
        }
    }
    if (state.isPaused) {
        statusBadge += '<span class="badge badge-paused">⏸ PAUSED</span>';
    }

    const pauseLabel = state.isPaused ? 'Resume ▶' : 'Pause ⏸';
    const showPause = meta.isStreaming && !meta.isComplete;
    const pauseButtonHtml = showPause
        ? `<button class="btn-pause" style="min-width: 60px;">${pauseLabel}</button><span style="display:inline-block; width: 10px;"></span>`
        : '';

    const controls = `
    <div class="toolbar">
        <div class="btn-group">
            ${pauseButtonHtml}
            <button class="btn-first" ${cPage === 1 ? 'disabled' : ''}>&lt;&lt;</button>
            <button class="btn-prev" ${cPage === 1 ? 'disabled' : ''}>&lt;</button>
            <span style="margin: 0 5px;">Page ${cPage} of ${tPages}</span>
            <button class="btn-next" ${cPage === tPages ? 'disabled' : ''}>&gt;</button>
            <button class="btn-last" ${cPage === tPages ? 'disabled' : ''}>&gt;&gt;</button>
        </div>
        <div>
            ${statusBadge}
            <span class="info">Total: ${globalOffset + totalRows} (Buf: ${totalRows})</span>
            <select class="select-pagesize">
                <option value="10" ${pSize === 10 ? 'selected' : ''}>10 / page</option>
                <option value="20" ${pSize === 20 ? 'selected' : ''}>20 / page</option>
                <option value="50" ${pSize === 50 ? 'selected' : ''}>50 / page</option>
                <option value="100" ${pSize === 100 ? 'selected' : ''}>100 / page</option>
                <option value="1000" ${pSize === 1000 ? 'selected' : ''}>All (1000)</option>
            </select>
        </div>
    </div>
    `;

    element.innerHTML = `
        ${styles}
        ${controls}
        <div style="overflow-x: auto; max-height: 500px;">
            <table class="dataframe">
                <thead>${headerRow}</thead>
                <tbody>${body}</tbody>
            </table>
        </div>
    `;
}

export const activate: ActivationFunction = (context) => {
    return {
        renderOutputItem(data, element) {
            const json = data.json();

            if (!json || !json.data || !json.schema) {
                element.innerHTML = `<div style="color:red">No data or invalid format</div>`;
                return;
            }

            // Initialize state if needed
            let state = stateMap.get(element);
            if (!state) {
                state = {
                    pageSize: 10,
                    currentPage: 1,
                    autoScroll: true,
                    isPaused: false,
                    latestJson: json
                };
                stateMap.set(element, state);

                // Event Delegation
                element.addEventListener('click', (e: any) => {
                    const target = e.target as any;
                    const s = stateMap.get(element);
                    if (!s) {return;}

                    if (target.matches('.btn-prev')) {
                        if (s.currentPage > 1) {
                            s.currentPage--;
                            s.autoScroll = false;
                            render(element);
                        }
                    } else if (target.matches('.btn-next')) {
                        const total = Math.ceil((s.latestJson.data.length || 0) / s.pageSize);
                        if (s.currentPage < total) {
                            s.currentPage++;
                            if (s.currentPage === total && !s.isPaused) {s.autoScroll = true;}
                            render(element);
                        }
                    } else if (target.matches('.btn-first')) {
                        s.currentPage = 1;
                        s.autoScroll = false;
                        render(element);
                    } else if (target.matches('.btn-last')) {
                        const total = Math.ceil((s.latestJson.data.length || 0) / s.pageSize);
                        s.currentPage = total;
                        if (!s.isPaused) {s.autoScroll = true;}
                        render(element);
                    } else if (target.matches('.btn-pause')) {
                        s.isPaused = !s.isPaused;
                        // If resuming, re-enable autoscroll if we were at the end? 
                        // Let's just default to enabling autoscroll on resume for convenience, 
                        // OR keep it where it was.
                        // Better: If resuming, just show latest data.
                        render(element);
                    }
                });

                element.addEventListener('change', (e: any) => {
                    const target = e.target as any;
                    const s = stateMap.get(element);
                    if (!s) {return;}
                    if (target.matches('.select-pagesize')) {
                        s.pageSize = parseInt((target as any).value);
                        s.currentPage = 1;
                        s.autoScroll = true;
                        render(element);
                    }
                });
            }

            // Always update latest data
            state.latestJson = json;

            // Only render if not paused
            if (!state.isPaused) {
                render(element);
            }
        }
    };
};
