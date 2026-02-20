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
        SELECT users.name, orders.user_id
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        WHERE users.name = ['jorge']
    }
    catch (error){
        res.status(500).send('error en la bse de datos')
    }

})