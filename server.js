const express = require('express');
const mysql = require('mysql2')
const app = express()


app.use(express.json())

const pool = mysql.createPool({
    host: '157.180.40.190',             // servidor MySQL
    user: 'root',                      // usuario
    password: 'scORHWprCvp26Gz1zwPQgSsokHyPC2', // contraseña
    database: 'db_andrescortes_ejercicio',             // base de datos
    waitForConnections: true,             // si no hay conexiones, esperar
    connectionLimit: 10,                  // máximo de conexiones simultáneas
    queueLimit: 0                         // sin límite de cola
});

const promisePool = pool.promise();

app.get('/ordenes-usuarios', async (req , res)=>{
    try{
        const sql = 
        `SELECT users.name,users.email, orders.order_number
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        WHERE users.name = 'jorge'`;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

const port = 3002
app.listen(port, () => {
    console.log(`Servidor corriendo en http://localhost:${port}`);
});