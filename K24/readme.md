As per the ask , created the docker compose which will automcatically pull all the dependencies. As mentioned in the requirement we have 3 tasks so created 3 different code which are runnable (dockerized).

simple run the docker compose file
command:
docker-compose up -d

This will start pulling the dependencies and build/created an image and start running the containers.

Container:
jupyter-1
postgres
Once the container are running,it will creata a mounted folder Notebook where cleaned files and graph png will be placed.

Process Flow:
1-once the container start runnning it will pick sales_data.csv file using jupyter notebook(data_analysis.ipynb)
2-the above jupyter notebook will generate  a cleaned csv - sales_data_cleaned.csv and sales_price_vs_quantity_per_product.png in Notebook folder.
3- Now, sales_data_cleaned.csv is picked by Transfer_sales_Data.py script and start reading and then load it into postgresql database.
4- We have already created the container for postgresql in docker compose. 
5- Connection details will be available in the notebook or docker compose.
6- Once Transfer_sales_Data.py script is completed , Data will be loaded under K24_DB Database and Schema-Public and Table-sales_data
7- Now, app.py will be Triggered which will pick the data from sales_data Table and show the expected response.



