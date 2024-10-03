# README

---

## Running the PDF Merger App via Docker

### Prerequisites
1. **Docker** installed and running.
2. **Redis** and your application containers need to be run on the same Docker network.

### Steps for Running the App Locally

### Case 1: Running Redis and the App in the Same Terminal
1. **Activate the virtual environment**:
   ```bash
   .venv\Scripts\activate
   ```

2. **Create a Docker network**:
   This allows the Redis and app containers to communicate.
   ```bash
   docker network create pdf-merger-network
   ```

3. **Run Redis on the network**:
   Start a Redis container on the network.
   ```bash
   docker run -d --name redis --network pdf-merger-network -p 6379:6379 redis
   ```

4. **Build the app Docker image**:
   If the app is not yet built, build it using this command.
   ```bash
   docker build -t pdf-merger-app .
   ```

5. **Run the app on the network**:
   Now, run your app container and ensure it's on the same network as Redis.
   ```bash
   docker run -e PORT=8080 --network pdf-merger-network -p 8080:8080 pdf-merger-app
   ```

6. **Access the app**:
   Once the app is running, it should be available at `http://localhost:8080`.

---

### Case 2: Using Two Separate Terminals (If Preferred)

#### Terminal 1:
1. **Activate the virtual environment**:
   ```bash
   .venv\Scripts\activate
   ```

2. **Create the Docker network**:
   (Only needs to be done once)
   ```bash
   docker network create pdf-merger-network
   ```

3. **Run Redis on the network**:
   ```bash
   docker run -d --name redis --network pdf-merger-network -p 6379:6379 redis
   ```

#### Terminal 2:
1. **Build the app Docker image**:
   ```bash
   docker build -t pdf-merger-app .
   ```

2. **Run the app container on the network**:
   ```bash
   docker run -e PORT=8080 --network pdf-merger-network -p 8080:8080 pdf-merger-app
   ```

### Important Notes:
- **REDIS_HOST** in your `.env` or `docker-compose.yaml` should be set to `"redis"`, as Redis will be accessible by this hostname when running in the same network.
  