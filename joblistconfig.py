pkl_map=dict({
    'root':[
        {
            'tablename':'customer',
            'create_statement':"""CREATE TABLE IF NOT EXISTS customer
            (
            id INT PRIMARY KEY NOT NULL,
            name VARCHAR(30) NOT NULL,
            username VARCHAR(30),
            email VARCHAR(30),
            phone VARCHAR(30),
            street VARCHAR(30),
            suite VARCHAR(30),
            city VARCHAR(15) NOT NULL,
            country VARCHAR(20),
            zipcode VARCHAR(10),
            cust_lat FLOAT,
            cust_lng FLOAT,
            city_lon FLOAT,
            city_lat FLOAT,
            total_price_per_customer FLOAT,
            avg_price_per_customer FLOAT,
            total_orders_per_customer FLOAT,
            times_orders_per_customer INT,
            total_price_per_cntry FLOAT
            )"""
        },
        {
            'tablename':'company',
            'create_statement':""" CREATE TABLE IF NOT EXISTS company
            (
            id INT NOT NULL,
            company_name VARCHAR(30) PRIMARY KEY NOT NULL,
            company_catchphrase VARCHAR(100),
            company_bs VARCHAR(100),
            FOREIGN KEY (id) REFERENCES customer(id)
            )
            """
        },
        {
            'tablename':'weather',
            'create_statement':""" CREATE TABLE IF NOT EXISTS weather
            (
            city VARCHAR(15) PRIMARY KEY NOT NULL,
            base VARCHAR(20),
            visibility FLOAT,
            dt FLOAT,
            timezone INT,
            city_id INT,
            cod FLOAT,
            temp FLOAT,
            feels_like_temp FLOAT,
            temp_min FLOAT,
            temp_max FLOAT,
            pressure FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            wind_deg FLOAT,
            clouds FLOAT,
            weather_id FLOAT,
            main_status VARCHAR(20),
            description VARCHAR(30),
            total_price_per_main_w FLOAT,
            total_price_per_temp FLOAT,
            total_price_per_desc_w FLOAT,
            total_price_per_pressure FLOAT,
            total_price_per_humidity FLOAT
            )
            """
        },
        {
            'tablename':'orders',
            'create_statement':"""CREATE TABLE IF NOT EXISTS orders
            (
            id INT NOT NULL,
            order_id INT,
            product_id INT,
            quantity INT,
            price FLOAT,
            order_date DATE,
            order_date_day INT,
            order_date_mnth INT,
            order_date_week INT,
            order_date_year INT,
            order_date_yr_mnth VARCHAR(10),
            FOREIGN KEY (id) REFERENCES customer(id)
            )
            """
        }
    ]
})