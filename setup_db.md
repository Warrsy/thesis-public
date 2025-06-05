
### **Step 1: Start a PostgreSQL Docker Container**

We'll use the official PostgreSQL image from Docker Hub to create our database container.

```Bash
docker run --name trino-postgres -p 5432:5432 -e POSTGRES_USER=trino -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=trinodb -d postgres
```

- `docker run`: This command initiates the creation and running of a new Docker container.
- `--name trino-postgres`: We're assigning the name "trino-postgres" to this specific container, making it easier to refer to later.
- `-p 5432:5432`: This maps port 5432 on your local machine to port 5432 inside the container. PostgreSQL uses port 5432 by default for communication.
- `-e POSTGRES_USER=trino`: This sets an environment variable inside the container that PostgreSQL will use to create a user named "trino".
- `-e POSTGRES_PASSWORD=secret`: Similarly, this sets the password for the "trino" user to "secret". **Important:** For production environments, you should use a more secure password management strategy.
- `-e POSTGRES_DB=trinodb`: This creates a database named "trinodb" within the PostgreSQL instance.
- `-d postgres`: This tells Docker to use the official "postgres" image from Docker Hub and run the container in detached mode (in the background), so your terminal remains free.
### **Step 2: Start the Trino Docker Container (If Not Already Running)**
Now, let's get Trino running in its own container. We'll use the official Trino Docker image from Docker Hub.
```Bash
docker run --name trino-server -p 8080:8080 trinodb/trino:latest
```
- `docker run`: Again, this starts a new Docker container.
- `--name trino-server`: We're naming this container "trino-server".
- `-p 8080:8080`: This maps port 8080 on your local machine to port 8080 inside the container. Trino's web UI and client connections use this port by default.
- `trinodb/trino:latest`: This instructs Docker to use the latest stable version of the Trino image provided by the `trinodb` organization on Docker Hub.
### **Step 3: Access the Trino Container's Shell**
To configure Trino to connect to our PostgreSQL database, we need to access the Trino container's file system.

```Bash
docker exec -it trino-server bash
```
**Explanation of the command:**

- `docker exec`: This command allows you to run commands inside a running Docker container.
- `-it`: These flags provide an interactive terminal (`-i`) and allocate a pseudo-TTY (`-t`), giving you a shell inside the container.
- `trino-server`: This specifies the target container where we want to execute the command.
- `bash`: This is the shell program we want to run inside the container.
### **Step 4: Create the PostgreSQL Catalog Configuration File**
Inside the Trino container's shell (you'll see a prompt like `[trino@...]`), we'll navigate to the directory where Trino looks for catalog configuration files and create the file for our PostgreSQL connection.

```Bash
cd /etc/trino/catalog
```

**Explanation of the command:**

- `cd`: This command stands for "change directory".
- `/etc/trino/catalog`: This is the directory within the Trino container where Trino expects to find configuration files for different data sources (catalogs).

Now, let's use the `echo` command to create the `postgresql.properties` file and add the necessary connection details.

**Inside the Trino container's shell (`[trino@...]`):**

1. **Create the `postgresql.properties` file and add the first line:**
	```Bash
    echo "connector.name=postgresql" > postgresql.properties
    ```
    - `echo "connector.name=postgresql"`: This command outputs the text `"connector.name=postgresql"`.
	- `>`: This redirects the output of the `echo` command to a file named `postgresql.properties`, creating the file if it doesn't exist and overwriting it if it does.
	- `connector.name=postgresql`: This line tells Trino to use the PostgreSQL connector to interact with our PostgreSQL database.
2. **Append the connection URL:**
    
    ```Bash
    echo "connection-url=jdbc:postgresql://host.docker.internal:5432/trinodb" >> postgresql.properties
    ```
    - `echo "connection-url=jdbc:postgresql://host.docker.internal:5432/trinodb"`: This outputs the JDBC URL required to connect to the PostgreSQL database.
	- `>>`: This redirects the output of the `echo` command and appends it to the `postgresql.properties` file.
	- `connection-url`: This property specifies the JDBC connection string.
	- `jdbc:postgresql://host.docker.internal:5432/trinodb`: This is the specific URL.
	    - `jdbc:postgresql://`: Indicates that we're using the PostgreSQL JDBC driver.
	    - `host.docker.internal`: This is a special DNS name provided by Docker Desktop that allows containers to connect to the host machine. **Note:** If you are not using Docker Desktop, you might need to replace this with the IP address of your host machine or the network name/IP address of the `trino-postgres` container. This is a common point of confusion, so be mindful of your Docker setup.
	    - `:5432`: Specifies the port PostgreSQL is running on (the default).
	    - `/trinodb`: Indicates the name of the database we want to connect to.
3. **Append the username:**
    
    ```Bash
    echo "connection-user=trino" >> postgresql.properties
    ```
    * `connection-user=trino`: This line specifies the username Trino should use to connect to PostgreSQL.
4. **Append the password:**
    
    ```Bash
    echo "connection-password=secret" >> postgresql.properties
    ```
	- `connection-password=secret`: This line provides the password for the "trino" PostgreSQL user.
### **Step 5: Connect to the Trino CLI**
Now that Trino is configured to talk to PostgreSQL, let's connect to the Trino command-line interface (CLI) to interact with it. Open a **new terminal window** on your local machine (not inside the Docker container).

```Bash
./trino-cli --server localhost:8080
```
**Explanation of the command:**

- `./trino-cli`: This executes the Trino CLI program. You might need to adjust this path depending on where you downloaded or installed the Trino CLI. If you're running this from within the Trino container (though the instruction says a new terminal), you might need to adjust the path or ensure the CLI is accessible in the container's `PATH`.
- `--server localhost:8080`: This tells the Trino CLI the address and port of the Trino server we want to connect to. `localhost` refers to your local machine, and `8080` is the port we mapped in the `docker run` command.
### **Step 6: List Available Catalogs**
Once connected to the Trino CLI, you should see a prompt like `trino>`. Now you can execute SQL queries. Let's check if Trino has successfully recognized our PostgreSQL catalog.

```SQL
SHOW CATALOGS;
```

You should now see a `postgresql` catalog in the list.
### **Step 7: Query Your PostgreSQL Database**

Now you can interact with the tables in your `trinodb` database through the `postgresql` catalog. Replace `{table}` with the actual name of a table in your PostgreSQL database.

```SQL
SELECT * FROM postgresql.public.{table};
```

---
If you want to access the postgres database you can do so with this command
```bash
### To log into the postgres database
psql -h localhost -p 5432 -d trinodb -U trino -W
```
With password `secret`.