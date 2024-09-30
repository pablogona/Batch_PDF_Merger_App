## **Running the PDF Merger App Locally**

### Option 1: Using a Single Terminal
This method is quick and suitable for simple development, but logs from Redis and the app will be mixed in one terminal.

1. **Activate the virtual environment.**
   ```bash
   .venv\Scripts\activate
   ```

2. **Start Redis and run the app inside Docker.**
   In a single terminal, you can run both Redis and your app sequentially:
   ```bash
   docker run -d -p 6379:6379 --name redis redis
   docker build -t pdf-merger-app .
   docker run -e PORT=8080 -p 8080:8080 pdf-merger-app
   ```

3. **Access the app in your browser.**
   Navigate to `http://localhost:8080` in your browser.

---

### Option 2: Using Two Terminals
This method separates Redis and the app into two terminals, making it easier to monitor and control each service independently.

1. **Terminal 1: Start Redis via Docker.**
   ```bash
   .venv\Scripts\activate
   docker run -d -p 6379:6379 --name redis redis
   ```

2. **Terminal 2: Build and run your app inside Docker.**
   In a second terminal, you’ll build the Docker image and run your app:
   ```bash
   docker build -t pdf-merger-app .
   docker run -e PORT=8080 -p 8080:8080 pdf-merger-app
   ```

3. **Access the app in your browser.**
   Navigate to `http://localhost:8080` in your browser.

---

### **Advantages of Using Two Terminals:**
- **Monitor logs separately**: You can view Redis logs in one terminal and the app logs in another for easier debugging.
- **Restart services independently**: You can restart Redis or the app independently without affecting the other service.
- **Control over long-running services**: If Redis or your app crashes, you can easily monitor and restart one without disrupting the other.

### **When to use each approach:**
- **Single Terminal**: Use this when doing simple development or quick tests.
- **Two Terminals**: Use this when you need better control, logging, and management of services.
