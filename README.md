# Omie API Integration

This repository provides a Python-based integration with the Omie API. The project fetches data from various Omie API endpoints, cleans the data by removing unwanted fields, and stores the processed results into a PostgreSQL database.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [How It Works](#how-it-works)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Features

- **API Integration**: Retrieves data from Omie API endpoints.
- **Pagination Handling**: Automatically iterates through multiple pages based on the API's response.
- **Data Cleaning**: Removes unnecessary fields (e.g., `tags`, `recomendacoes`, `homepage`, `fax_ddd`, `bloquear_exclusao`, `produtor_rural`) from the API response.
- **Database Storage**: Uses Pandas and SQLAlchemy to store data into a PostgreSQL database.
- **Optional File Saving**: Provides an option to save the API response as JSON files.

## Prerequisites

- **Python 3.7+**
- **PostgreSQL**: A running PostgreSQL instance.
- Required Python libraries:
  - `pandas`
  - `sqlalchemy`
  - `requests` (used by the custom API class in `src/api.py`)

Install the Python dependencies via pip:
```bash
pip install pandas sqlalchemy requests
````

## Installation
Clone the repository:
```
git clone https://github.com/rphpacheco/omie_api_integration.git
```
Navigate to the project directory:

```
cd omie_api_integration
```

(Optional) Create and activate a virtual environment:

```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
````

Install dependencies: If a requirements.txt file is provided, run:

```
pip install -r requirements.txt
```

Otherwise, install the dependencies manually as shown above.

## Configuration
Environment Variables:

Create a `.env` file in the root directory using the provided `.env-pattern` as a template:

```
cp .env-pattern .env
```

Edit the .env file with your credentials:

```
APP_KEY=your_app_key_here
APP_SECRET=your_app_secret_here
BASE_URL=https://api.omie.com.br/api/v1/
DB_HOST=your_db_host
DB_PORT=your_db_port
DB_USERNANE=your_db_username   # Note: The variable name is 'DB_USERNANE' in this project.
DB_PASSWORD=your_db_password
DB_NAME=your_db_name
```

How It Works

Configuration & Setup:
The project loads settings from the environment and uses a custom configuration class (src/config.py) to manage API and database credentials.

Fetching Endpoints:
Endpoints are defined and retrieved via the Endpoints class (src/endpoints.py), which provides a list of API endpoints to be queried.

Data Retrieval & Pagination:
For each endpoint, the script:

Determines the total number of pages available by making an initial API request.
Iterates through each page, updating the page parameter in the request.
Sends a POST request to the Omie API using the custom Api class (src/api.py).
Data Processing:
After receiving the data, the script:

Removes unwanted fields using a predefined blacklist.
Normalizes the JSON data using Pandas.
Data Storage:
The processed data is stored in a PostgreSQL database:

For the first page of data, the corresponding table is created (or replaced).
For subsequent pages, the data is appended to the table.
File Saving Option:
There is also functionality to save the raw JSON response to a file.

Usage

Run the main integration script with:

```
python main.py
```

As the script runs, it will:

Connect to the Omie API using your credentials.
Retrieve and process data from each endpoint.
Store the data in your PostgreSQL database.
Output progress messages to the console, including the total pages and records fetched.
Contributing
Contributions are welcome! If you have suggestions or improvements, feel free to fork the repository and submit a pull request.

License
This project does not specify a license. Please contact the repository owner for more details.