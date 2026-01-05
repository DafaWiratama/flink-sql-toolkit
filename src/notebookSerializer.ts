import * as vscode from 'vscode';
import { TextDecoder, TextEncoder } from 'util';

interface RawNotebookCell {
    language: string;
    value: string;
    kind: vscode.NotebookCellKind;
}

interface RawNotebook {
    cells: RawNotebookCell[];
}

export class FlinkSqlNotebookSerializer implements vscode.NotebookSerializer {
    async deserializeNotebook(
        content: Uint8Array,
        _token: vscode.CancellationToken
    ): Promise<vscode.NotebookData> {
        const contents = new TextDecoder().decode(content);

        let raw: RawNotebook = { cells: [] };
        try {
            raw = <RawNotebook>JSON.parse(contents);
        } catch {
            // Handle new/empty file
            raw = { cells: [] };
        }

        const cells = raw.cells.map(item => new vscode.NotebookCellData(
            item.kind,
            item.value,
            item.language
        ));

        // If it's a new empty file, give it one empty cell
        if (cells.length === 0) {
            cells.push(new vscode.NotebookCellData(vscode.NotebookCellKind.Code, '', 'sql'));
        }

        return new vscode.NotebookData(cells);
    }

    async serializeNotebook(
        data: vscode.NotebookData,
        _token: vscode.CancellationToken
    ): Promise<Uint8Array> {
        const contents: RawNotebook = {
            cells: data.cells.map(cell => ({
                kind: cell.kind,
                language: cell.languageId,
                value: cell.value
            }))
        };

        return new TextEncoder().encode(JSON.stringify(contents, null, 2));
    }
}
