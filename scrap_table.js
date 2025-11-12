const chromium = require('@sparticuz/chromium');
const puppeteer = require('puppeteer-core');
const AWS = require('aws-sdk');

const dynamodb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    const url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados";
    
    let browser;
    try {
        browser = await puppeteer.launch({
            args: chromium.args,
            defaultViewport: chromium.defaultViewport,
            executablePath: await chromium.executablePath(),
            headless: chromium.headless,
        });
        
        const page = await browser.newPage();
        await page.goto(url);
        await page.waitForSelector('table');
        
        const rows = await page.evaluate(() => {
            const headers = Array.from(document.querySelectorAll('th')).map(h => h.textContent.trim());
            const rows = [];
            document.querySelectorAll('tr').forEach((row, idx) => {
                if (idx === 0) return;
                const cells = Array.from(row.querySelectorAll('td')).map(c => c.textContent.trim());
                if (cells.length > 0) {
                    const rowData = {};
                    headers.forEach((header, i) => {
                        rowData[header] = cells[i] || '';
                    });
                    rows.push(rowData);
                }
            });
            return rows;
        });
        
        // Guardar en DynamoDB
        const params = {
            TableName: 'TablaWebScrapping2',
            Item: {
                id: Date.now().toString(),
                data: rows,
                timestamp: new Date().toISOString()
            }
        };
        
        await dynamodb.put(params).promise();
        
        return {
            statusCode: 200,
            body: JSON.stringify(rows)
        };
    } catch (error) {
        return {
            statusCode: 500,
            body: JSON.stringify({ error: error.message })
        };
    } finally {
        if (browser) await browser.close();
    }
};