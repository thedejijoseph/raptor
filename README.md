### Raptor

A server to fetch, parse, and return data from listed APIs

- GitHub
- Notion

Server is built (to be asynchronous) using Uvicorn & FastAPI

### Setup
Provide authentication and access credentials using [this format](src/.env.example).

Run server using `uvicorn main:app --reload` or `heroku local` in your dev environment.

The provided [Procfile](Procfile) will handle startup on an Heroku server.
