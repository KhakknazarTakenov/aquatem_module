const fs = require('fs');
const path = require('path');

function logError(source, error) {
    try {
        const currentTime = new Date().toLocaleString();
        const errorMessage = `${currentTime} - Source: ${source}\nError: ${error?.stack || error}\n\n`;
        const logsDir = path.join(__dirname, 'logs', 'error');

        // Check if the error logs directory exists, if not, create it
        if (!fs.existsSync(logsDir)) {
            fs.mkdirSync(logsDir, { recursive: true }); // Use recursive to create nested directories
        }

        const logFilePath = path.join(logsDir, `error_${formatDate(new Date())}.log`);

        fs.appendFile(logFilePath, errorMessage, (err) => {
            if (err) {
                console.error('Ошибка записи в файл:', err);
            } else {
                console.log(`${currentTime} Err wrote to the file`);
            }
        });
    } catch (error) {
        console.error(error);
    }
}

function logAccess(source, message) {
    try {
        const currentTime = new Date().toLocaleString();
        const errorMessage = `${currentTime} - Source: ${source}\nMessage: ${message}\n\n`;
        const logsDir = path.join(__dirname, 'logs');

        if (!fs.existsSync(logsDir)) {
            fs.mkdirSync(logsDir);
        }
        const logFilePath = path.join(logsDir, "access", `access_${formatDate(new Date())}.log`);

        fs.appendFile(logFilePath, errorMessage, (err) => {
            if (err) {
                console.error('Err writing in file:', err);
            } else {
                console.log(`${currentTime} New access log`);
            }
        });
    } catch (error) {
        console.error(error);
    }
}

function formatDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0'); // Add 1 because getMonth() is zero-based
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}_${month}_${day}`;
}

module.exports = { logError, logAccess }