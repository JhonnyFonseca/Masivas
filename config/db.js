const mysql = require('mysql2/promise');

const dbConfig = {
    host: 'localhost',
    port: 3307,
    user: 'root',
    password: 'sql-bdm-pssw',
    database: 'secop_contratos',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    charset: 'utf8mb4'
};

const pool = mysql.createPool(dbConfig);

async function executeQuery(sql, params = []) {
    try {
        const [rows] = await pool.execute(sql, params);
        return rows;
    } catch (error) {
        console.error('Error ejecutando query:', error);
        throw error;
    }
}

async function executeTransaction(queries) {
    const connection = await pool.getConnection();
    await connection.beginTransaction();
    
    try {
        const results = [];
        for (const { sql, params } of queries) {
            const [result] = await connection.execute(sql, params || []);
            results.push(result);
        }
        
        await connection.commit();
        connection.release();
        return results;
    } catch (error) {
        await connection.rollback();
        connection.release();
        throw error;
    }
}

async function executeQuery(sql, params = []) {
    try {
        const [rows] = await pool.execute(sql, params);
        return rows;
    } catch (error) {
        console.error('Error ejecutando query:', error);
        throw error;
    }
}

async function executeTransaction(queries) {
    const connection = await pool.getConnection();
    await connection.beginTransaction();
    
    try {
        const results = [];
        for (const { sql, params } of queries) {
            const [result] = await connection.execute(sql, params || []);
            results.push(result);
        }
        
        await connection.commit();
        connection.release();
        return results;
    } catch (error) {
        await connection.rollback();
        connection.release();
        throw error;
    }
}

module.exports = {
    pool,
    testConnection,
    executeQuery,
    executeTransaction,
    dbConfig
};