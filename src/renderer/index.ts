

import type { ActivationFunction } from 'vscode-notebook-renderer';

// State to persist per output element
interface RenderState {
    pageSize: number;
    currentPage: number;
    autoScroll: boolean;
    isPaused: boolean;
    latestJson: any;
    // New features state
    sortColumn: string | null;
    sortDirection: 'asc' | 'desc';
    filterText: string;
}

const stateMap = new WeakMap<any, RenderState>();

// Helper to render the view based on state
function render(element: any) {
    const state = stateMap.get(element);
    if (!state || !state.latestJson) { return; }

    const json = state.latestJson;
    let rows = [...(json.data || [])]; // Copy for mutation
    const columns = json.schema.fields || [];
    const meta = json.metadata || {};
    const globalOffset = typeof meta.offset === 'number' ? meta.offset : 0;

    // 1. Filter
    if (state.filterText) {
        const lowerFilter = state.filterText.toLowerCase();
        rows = rows.filter(row => {
            const fields = row.fields || (Array.isArray(row) ? row : []);
            return fields.some((f: any) => String(f).toLowerCase().includes(lowerFilter));
        });
    }

    // 2. Sort
    if (state.sortColumn) {
        const colIdx = columns.findIndex((c: any) => c.name === state.sortColumn);
        if (colIdx !== -1) {
            rows.sort((a, b) => {
                const valA = (a.fields || a)[colIdx];
                const valB = (b.fields || b)[colIdx];

                if (valA === valB) { return 0; }
                const result = valA > valB ? 1 : -1;
                return state.sortDirection === 'asc' ? result : -result;
            });
        }
    }

    // Auto-scroll logic (only if not paused and autoScroll enabled)
    const totalRows = rows.length;
    const totalPages = Math.ceil(totalRows / state.pageSize) || 1;

    if (state.autoScroll && !state.isPaused) {
        state.currentPage = totalPages;
    } else {
        if (state.currentPage > totalPages) { state.currentPage = totalPages; }
        if (state.currentPage < 1) { state.currentPage = 1; }
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
                        box-shadow: 0 2px 5px var(--vscode-widget-shadow, rgba(0,0,0,0.1));
                        cursor: pointer;
                        user-select: none;
                    }
                    .dataframe th:hover { background: var(--vscode-list-hoverBackground); }
                    
                    .dataframe th:first-child, .dataframe td:first-child { 
                        position: sticky; 
                        left: 0; 
                        border-right: 2px solid var(--vscode-editorGroup-border);
                        background: var(--vscode-editor-background); 
                        font-weight: bold; 
                        z-index: 11;
                    }
                    .dataframe th:first-child { z-index: 12; cursor: default; }
                    
                    .dataframe tr:nth-child(even) td { background: var(--vscode-notebook-cellEditorBackground); }
                    .dataframe tr:hover td { background: var(--vscode-list-hoverBackground); }
                    
                    .toolbar { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 8px; margin-bottom: 5px; font-size: 11px; font-family: var(--vscode-font-family); padding: 4px; background: var(--vscode-editor-background); position: sticky; top: 0; left: 0; border-bottom: 1px solid var(--vscode-widget-border); }
                    .btn-group { display: flex; align-items: center; }
                    .btn-group button { background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; padding: 4px 8px; cursor: pointer; margin-right: 2px; }
                    .btn-group button:hover { background: var(--vscode-button-secondaryHoverBackground); }
                    .btn-group button:disabled { opacity: 0.5; cursor: not-allowed; }
                    
                    .filter-box { 
                        background: var(--vscode-input-background); 
                        color: var(--vscode-input-foreground); 
                        border: 1px solid var(--vscode-input-border); 
                        padding: 3px 6px;
                        font-family: inherit;
                        font-size: inherit;
                        width: 150px;
                    }
                    
                    .select-pagesize { background: var(--vscode-dropdown-background); color: var(--vscode-dropdown-foreground); border: 1px solid var(--vscode-dropdown-border); }
                    .info { color: var(--vscode-descriptionForeground); margin-left: 10px; font-size: 10px; }
                    
                    .badge { padding: 2px 6px; border-radius: 3px; font-weight: bold; font-size: 10px; margin-right: 5px; }
                    .badge-live { background-color: var(--vscode-button-background); color: white; }
                    .badge-finished { background-color: var(--vscode-testing-iconPassed); color: white; }
                    .badge-paused { background-color: var(--vscode-inputValidation-warningBackground); color: var(--vscode-inputValidation-warningForeground); border: 1px solid var(--vscode-inputValidation-warningBorder); }
                    
                    .sort-icon { font-size: 9px; margin-left: 4px; }
                </style>
                `;

    // Header with Sort Indicators
    const headerCells = columns.map((c: any) => {
        let sortIndicator = '';
        if (state.sortColumn === c.name) {
            sortIndicator = state.sortDirection === 'asc' ? ' ▲' : ' ▼';
        }
        return `<th data-col="${c.name}">${c.name}<span class="sort-icon">${sortIndicator}</span></th>`;
    }).join('');
    const headerRow = `<tr><th>#</th>${headerCells}</tr>`;

    // Body
    const body = pageRows.map((row: any, i: number) => {
        // Global index is tricky with weak filters. 
        // If sorting or filtering is active, listing "original index" is hard without tracking it.
        // We'll show relative index + info that filtered.
        const displayIndex = startIdx + i + 1;

        let cellData: any[] = [];
        if (row.fields && Array.isArray(row.fields)) { cellData = row.fields; }
        else if (Array.isArray(row)) { cellData = row; }
        else { cellData = columns.map((c: any) => row[c.name]); }

        const cells = cellData.map((v: any) => `<td>${v === null || v === undefined ? '' : String(v)}</td>`).join('');
        return `<tr><td>${displayIndex}</td>${cells}</tr>`;
    }).join('');

    // Toolbar Elements
    const pSize = state.pageSize;
    const cPage = state.currentPage;
    const tPages = totalPages;

    // Status Logic
    let statusBadge = '';
    if (meta.isStreaming) {
        statusBadge = meta.isComplete
            ? '<span class="badge badge-finished">● FINISHED</span>'
            : '<span class="badge badge-live">● LIVE</span>';
    }
    if (state.isPaused) {
        statusBadge += '<span class="badge badge-paused">⏸ PAUSED</span>';
    }

    const pauseLabel = state.isPaused ? 'Resume ▶' : 'Pause ⏸';
    const showPause = meta.isStreaming && !meta.isComplete;
    const pauseButtonHtml = showPause
        ? `<button class="btn-pause" style="min-width: 60px;">${pauseLabel}</button><span style="display:inline-block; width: 10px;"></span>`
        : '';

    // Determine info text
    let infoText = `Total: ${totalRows}`;
    if (json.data && json.data.length !== totalRows) {
        infoText += ` (Filtered from ${json.data.length})`;
    } else if (globalOffset > 0) {
        infoText += ` (Offset: ${globalOffset})`;
    }

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
        
        <div style="flex-grow: 1; text-align: right; padding-right: 10px;">
             <input type="text" class="filter-box" placeholder="Filter..." value="${state.filterText || ''}">
        </div>

        <div class="btn-group">
             <button class="btn-export-csv" title="Download CSV">CSV</button>
             <button class="btn-export-json" title="Copy JSON">JSON</button>
        </div>

        <div>
            ${statusBadge}
            <span class="info">${infoText}</span>
            <select class="select-pagesize">
                <option value="10" ${pSize === 10 ? 'selected' : ''}>10 / p</option>
                <option value="20" ${pSize === 20 ? 'selected' : ''}>20 / p</option>
                <option value="50" ${pSize === 50 ? 'selected' : ''}>50 / p</option>
                <option value="100" ${pSize === 100 ? 'selected' : ''}>100 / p</option>
                <option value="1000" ${pSize === 1000 ? 'selected' : ''}>All</option>
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

    // Put focus back on filter? (Tricky with innerHTML replacement)
    // If we want input focus persistence, we should do DOM diffing, but innerHTML is simpler for now.
    // We can just focus at end if it was focused.
    const input = element.querySelector('.filter-box');
    if (input && state.filterText && document.activeElement && (document.activeElement as any).className === 'filter-box') {
        input.focus();
        // Move cursor to end
        const val = input.value;
        input.value = '';
        input.value = val;
    }
}

function downloadCSV(state: RenderState) {
    const json = state.latestJson;
    if (!json || !json.data) { return; }

    // Use filtered/sorted rows logic if we want "What You See Is What You Get" export
    // But usually people want raw unique data. Let's filter but maybe not page.
    let rows = [...json.data];
    if (state.filterText) {
        const lowerFilter = state.filterText.toLowerCase();
        rows = rows.filter(row => {
            const fields = row.fields || (Array.isArray(row) ? row : []);
            return fields.some((f: any) => String(f).toLowerCase().includes(lowerFilter));
        });
    }

    const cols = json.schema.fields.map((f: any) => f.name);
    const header = cols.join(',');
    const body = rows.map((r: any) => {
        const fields = r.fields || r;
        return fields.map((v: any) => {
            if (v === null || v === undefined) { return ''; }
            const s = String(v).replace(/"/g, '""');
            if (s.includes(',') || s.includes('"') || s.includes('\n')) {
                return `"${s}"`;
            }
            return s;
        }).join(',');
    }).join('\n');

    const csvContent = header + '\n' + body;

    // Trigger download
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `flink_export_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}

function copyJSON(state: RenderState) {
    const json = state.latestJson;
    if (!json || !json.data) { return; }
    navigator.clipboard.writeText(JSON.stringify(json.data, null, 2));
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
                    latestJson: json,
                    sortColumn: null,
                    sortDirection: 'asc',
                    filterText: ''
                };
                stateMap.set(element, state);

                // Event Delegation
                element.addEventListener('click', (e: any) => {
                    const target = e.target as HTMLElement;
                    const s = stateMap.get(element);
                    if (!s) { return; }

                    // Sort Header Click
                    const th = target.closest('th');
                    if (th && th.dataset.col) {
                        const col = th.dataset.col;
                        if (s.sortColumn === col) {
                            s.sortDirection = s.sortDirection === 'asc' ? 'desc' : 'asc';
                        } else {
                            s.sortColumn = col;
                            s.sortDirection = 'asc';
                        }
                        render(element);
                        return; // Don't process other clicks
                    }

                    if (target.matches('.btn-prev')) {
                        if (s.currentPage > 1) {
                            s.currentPage--;
                            s.autoScroll = false;
                            render(element);
                        }
                    } else if (target.matches('.btn-next')) {
                        const totalRows = (s.latestJson.data || []).length; // Needs filtered count ideally, but approximation ok for event
                        const total = Math.ceil(totalRows / s.pageSize); // This will be recalculated in render anyway
                        // Just force update
                        s.currentPage++;
                        s.autoScroll = false;
                        render(element);
                    } else if (target.matches('.btn-first')) {
                        s.currentPage = 1;
                        s.autoScroll = false;
                        render(element);
                    } else if (target.matches('.btn-last')) {
                        s.currentPage = 999999;
                        if (!s.isPaused) { s.autoScroll = true; }
                        render(element);
                    } else if (target.matches('.btn-pause')) {
                        s.isPaused = !s.isPaused;
                        render(element);
                    } else if (target.matches('.btn-export-csv')) {
                        downloadCSV(s);
                    } else if (target.matches('.btn-export-json')) {
                        copyJSON(s);
                    }
                });

                element.addEventListener('input', (e: any) => {
                    const target = e.target as HTMLInputElement;
                    const s = stateMap.get(element);
                    if (!s) { return; }

                    if (target.matches('.filter-box')) {
                        s.filterText = target.value;
                        s.currentPage = 1; // Reset to first page on filter
                        render(element);
                        // Keep focus
                        const input = element.querySelector('.filter-box') as HTMLInputElement;
                        if (input) {
                            input.focus();
                        }
                    }
                });

                element.addEventListener('change', (e: any) => {
                    const target = e.target as any;
                    const s = stateMap.get(element);
                    if (!s) { return; }
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
                // If filter is active, updating data might move rows?
                // Just render.
                render(element);
            }
        }
    };
};
