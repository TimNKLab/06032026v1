# New Khatulistiwa Sales KPI Dashboard

This project is a Sales KPI dashboard for the retail cosmetic store New Khatulistiwa, built with Plotly Dash. It fetches data from an Odoo instance using odoorpc.

## MVP Implementation

This initial version (MVP) of the dashboard focuses on core UI functionality and a containerized setup.

### Features

- A multi-page Dash application using the `pages` feature.
- Synchronous data fetching from Odoo via `odoorpc`.
- A containerized environment using a multi-stage Dockerfile for optimized builds.
- A basic CI pipeline setup with GitHub Actions.

### Project Structure

```
.
├── .dockerignore
├── .github
│   └── workflows
│       └── main.yml
├── Dockerfile
├── app.py
├── odoorpc_connector.py
├── pages
│   ├── analytics.py
│   └── home.py
└── requirements.txt
```

### Configuration

The Odoo connection parameters are loaded from a local `.env` file using `python-dotenv`.

1. Create a file named `.env` in the root directory.
2. Add the following variables, replacing the placeholders with your Odoo SaaS connection details (the connector falls back to the values shown below if you omit the port/protocol):
```
ODOO_HOST=your_odoo_host
ODOO_DB=your_database_name
ODOO_USERNAME=your_login
ODOO_API_KEY=your_api_key
ODOO_PORT=443
ODOO_PROTOCOL=jsonrpc+ssl
```

### How to Run

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the application:**
    ```bash
    python app.py
    ```

3.  **Build the Docker image:**
    ```bash
    docker build . -t nk-sales-dashboard
    ```

4.  **Run the container:**
    ```bash
    docker run -p 8050:8050 nk-sales-dashboard
