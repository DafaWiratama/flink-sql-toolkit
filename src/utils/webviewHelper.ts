
export class WebviewHelper {
    public static getFrameHtml(url: string): string {
        return `
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
    }
}
