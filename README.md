# pythonicMySQL

A very simple [ORM](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping) Python library that allows access to data in a MySQL database without having to write SQL queries. I wrote this library when I wanted to simplify accessing a MySQL database I was managing. This was before I knew that many such libraries already existed. 

**I built this library for a very narrow purpose. If you are looking for a serious ORM solution, consider [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy) instead.**

## What have I learned from this project?

- Writing the logic for generating SQL queries from Python statements
- Throwing errors when appropriate and annotating function arguments and returns with their datatype to make code less error-prone
- Applying the separation of concerns principle; actively using the object-oriented programming model
- Gaining a deeper understanding of Relational DB Management Systems like MySQL
