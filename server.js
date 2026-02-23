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

app.get('/1-ej', async (req , res)=>{
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

app.get('/2-ej', async (req , res)=>{
    try{
        const sql = 
        `SELECT orders.order_number,orders.order_date, users.email
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        WHERE users.email = 'guillermo.gracia@iglesias.com'`;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/3-ej', async (req , res)=>{
    try{
        const sql = 
        `SELECT products.name , products.category_id
        FROM products
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/4-ej', async (req , res)=>{
    try{
        const sql = 
        `SELECT users.name, orders.user_id
        FROM users
        LEFT JOIN orders ON users_name = orders.user_id
        WHERE orders.user_id IS NULL
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/5-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT users.name, SUM(orders.total) 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        WHERE users.name = 'Jorge' AND orders.status = 'paid' 
        GROUP BY users.id, users.name;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/6-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT status, COUNT(*) 
        FROM orders 
        GROUP BY status;    
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/7-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT products.name, products.price 
        FROM products 
        INNER JOIN categories ON products.category_id = categories.id 
        WHERE categories.name = 'Electrónica' 
        ORDER BY products.price DESC;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/8-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT order_product.product_id, order_product.quantity
        FROM order_product
        INNER JOIN orders ON order_product.order_id = orders.id
        WHERE orders.order_number = 'ORD-2026-791GLM';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/9-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT DISTINCT users.name
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        WHERE users.city = 'Haro de Lemos';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/10-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT users.name, AVG(orders.total)
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        GROUP BY users.name
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

////////////////////////////////////////////////////////////////////////////////////////////////////////


app.get('/11-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT orders.order_number, orders.order_date, products.name, order_product.price
        FROM orders
        INNER JOIN order_product ON 
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/12-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT categories.name, SUM(order_product.quantity * order_product.price_at_purchase) 
        FROM categories 
        INNER JOIN products ON categories.id = products.category_id 
        INNER JOIN order_product ON products.id = order_product.product_id 
        GROUP BY categories.name; 
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/13-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT DISTINCT products.name 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        INNER JOIN products ON order_product.product_id = products.id 
        WHERE users.name = 'Jorge'; 
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/14-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT products.name, SUM(order_product.quantity) 
        FROM products 
        INNER JOIN order_product ON products.id = order_product.product_id 
        GROUP BY products.name 
        ORDER BY SUM(order_product.quantity) DESC 
        LIMIT 5;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/15-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT products.name, MAX(orders.order_date) 
        FROM products 
        INNER JOIN order_product ON products.id = order_product.product_id 
        INNER JOIN orders ON order_product.order_id = orders.id 
        GROUP BY products.name;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/16-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT DISTINCT users.name 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        INNER JOIN products ON order_product.product_id = products.id 
        WHERE products.name LIKE '%Gamer%';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/17-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT DATE(orders.order_date), SUM(orders.total) 
        FROM orders 
        WHERE orders.status = 'paid' 
        GROUP BY DATE(orders.order_date);
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/18-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT categories.name 
        FROM categories 
        INNER JOIN products ON categories.id = products.category_id 
        LEFT JOIN order_product ON products.id = order_product.product_id 
        WHERE order_product.id IS NULL 
        GROUP BY categories.name;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/19-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT users.name, AVG(orders.total) 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        GROUP BY users.name;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})



app.get('/20-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT DISTINCT products.name 
        FROM products 
        INNER JOIN order_product ON products.id = order_product.product_id 
        INNER JOIN orders ON order_product.order_id = orders.id 
        WHERE orders.status = 'cancelled';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})

app.get('/21-ej', async (req , res)=>{
    try{
        const sql = 
        ` 
        SELECT users.name, users.city, orders.order_number, products.name,categories.name, order_product.quantity, (order_product.quantity * order_product.price_at_purchase)
        FROM users
        INNER JOIN orders ON  users.id = orders.user_id 
        INNER JOIN order_product ON orders.id = order_product.order_id
        INNER JOIN products ON order_product.product_id = products.id
        INNER JOIN categories ON products.category_id = categories.id
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows)
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la bse de datos');
    }
})


app.get('/22-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT SUM(order_product.quantity * order_product.price_at_purchase) 
        FROM orders 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        INNER JOIN products ON order_product.product_id = products.id 
        INNER JOIN categories ON products.category_id = categories.id 
        INNER JOIN users ON orders.user_id = users.id 
        WHERE categories.name = 'Ropa y Moda' 
        AND users.city = 'Haro de Lemos' 
        AND orders.status = 'paid';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/23-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT users.name, SUM(orders.total) 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        WHERE orders.status = 'paid' 
        GROUP BY users.name 
        ORDER BY SUM(orders.total) DESC 
        LIMIT 1;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});



app.get('/24-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT products.name 
        FROM products 
        LEFT JOIN order_product ON products.id = order_product.product_id 
        WHERE order_product.id IS NULL;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/25-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT SUM((order_product.price_at_purchase - products.purchase_price) * order_product.quantity) 
        FROM order_product 
        INNER JOIN products ON order_product.product_id = products.id 
        INNER JOIN orders ON order_product.order_id = orders.id 
        WHERE orders.status = 'paid';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/26-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT DISTINCT users.name 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        INNER JOIN products ON order_product.product_id = products.id 
        INNER JOIN categories ON products.category_id = categories.id 
        WHERE categories.name = 'Consolas y Videojuegos' 
        AND users.id NOT IN (
            SELECT users.id 
            FROM users 
            INNER JOIN orders ON users.id = orders.user_id 
            INNER JOIN order_product ON orders.id = order_product.order_id 
            INNER JOIN products ON order_product.product_id = products.id 
            INNER JOIN categories ON products.category_id = categories.id 
            WHERE categories.name = 'Hogar y Electrodomésticos'
        );
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/27-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT users.city, SUM(orders.total) 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        WHERE orders.status = 'paid' 
        GROUP BY users.city 
        ORDER BY SUM(orders.total) DESC 
        LIMIT 3;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/28-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT orders.order_number, COUNT(DISTINCT order_product.product_id) 
        FROM orders 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        GROUP BY orders.order_number 
        ORDER BY COUNT(DISTINCT order_product.product_id) DESC 
        LIMIT 1;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/29-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT DISTINCT products.name 
        FROM products 
        INNER JOIN order_product ON products.id = order_product.product_id 
        WHERE order_product.price_at_purchase < products.sale_price;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/30-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT users.name, orders.order_date, order_product.price_at_purchase 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        INNER JOIN products ON order_product.product_id = products.id 
        WHERE products.name = 'dolore sed quia';
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/31-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT users.name 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        WHERE orders.status = 'paid' 
        GROUP BY users.name 
        HAVING SUM(orders.total) > (
            SELECT AVG(sub_total) 
            FROM (
                SELECT SUM(orders.total) AS sub_total 
                FROM orders 
                WHERE orders.status = 'paid' 
                GROUP BY orders.user_id
            ) AS promedios_subconsulta
        );
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/32-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT products.name 
        FROM products 
        INNER JOIN order_product ON products.id = order_product.product_id 
        INNER JOIN orders ON order_product.order_id = orders.id 
        WHERE orders.status = 'paid' 
        GROUP BY products.name 
        HAVING SUM(order_product.quantity * order_product.price_at_purchase) > (
            SELECT SUM(orders.total) * 0.02 
            FROM orders 
            WHERE orders.status = 'paid'
        );
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/33-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT DISTINCT users.name 
        FROM users 
        WHERE users.id NOT IN (
            SELECT orders.user_id 
            FROM orders 
            WHERE orders.order_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
        );
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/34-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT users.name, 
            CASE 
                WHEN SUM(orders.total) > 5000 THEN 'VIP' 
                WHEN SUM(orders.total) BETWEEN 1000 AND 5000 THEN 'Frecuente' 
                ELSE 'Regular' 
            END AS categoria_cliente 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        WHERE orders.status = 'paid' 
        GROUP BY users.name;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/35-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT DATE_FORMAT(orders.order_date, '%Y-%m') AS mes, SUM(orders.total) AS total_facturado
        FROM orders 
        WHERE orders.status = 'paid' 
        GROUP BY DATE_FORMAT(orders.order_date, '%Y-%m') 
        ORDER BY SUM(orders.total) DESC 
        LIMIT 1;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/36-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT DISTINCT orders.order_number 
        FROM orders 
        INNER JOIN order_product ON orders.id = order_product.order_id 
        INNER JOIN products ON order_product.product_id = products.id 
        WHERE orders.status = 'pending' 
        AND products.stock < 5;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/37-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT categories.name, 
        (SUM(order_product.quantity * order_product.price_at_purchase) / 
        (SELECT SUM(orders.total) FROM orders WHERE orders.status = 'paid') * 100) AS porcentaje_ventas 
        FROM categories 
        INNER JOIN products ON categories.id = products.category_id 
        INNER JOIN order_product ON products.id = order_product.product_id 
        INNER JOIN orders ON order_product.order_id = orders.id 
        WHERE orders.status = 'paid' 
        GROUP BY categories.name;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});

app.get('/38-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT users.city, SUM(orders.total) AS total_ciudad, 
        (SELECT AVG(sub_ciudad) 
        FROM(
            SELECT SUM(orders.total) AS sub_ciudad 
            FROM orders 
            INNER JOIN users ON orders.user_id = users.id 
            WHERE orders.status = 'paid' 
            GROUP BY users.city
        ) AS promedio_global_ciudades
        
        ) AS promedio_global 
        FROM users 
        INNER JOIN orders ON users.id = orders.user_id 
        WHERE orders.status = 'paid' 
        GROUP BY users.city;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/39-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT DATE_FORMAT(orders.order_date, '%Y-%m') AS mes, 
        (SUM(CASE WHEN orders.status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS tasa_cancelacion 
        FROM orders 
        GROUP BY DATE_FORMAT(orders.order_date, '%Y-%m');
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});


app.get('/40-ej', async (req , res)=>{
    try{
        const sql = 
        `
        SELECT t1.name AS producto_1, t2.name AS producto_2, COUNT(*) AS veces_comprados_juntos 
        FROM order_product AS op1 
        INNER JOIN order_product AS op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id 
        INNER JOIN products AS t1 ON op1.product_id = t1.id 
        INNER JOIN products AS t2 ON op2.product_id = t2.id 
        GROUP BY t1.name, t2.name 
        ORDER BY COUNT(*) DESC 
        LIMIT 1;
        `;

        const [rows] = await promisePool.query(sql);
        res.json(rows);
    }
    catch (error){
        console.error(error);
        res.status(500).send('error en la base de datos');
    }
});




const port = 3002
app.listen(port, () => {
    console.log(`Servidor corriendo en http://localhost:${port}`);
});