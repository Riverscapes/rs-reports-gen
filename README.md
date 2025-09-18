# Riverscapes Report Generators Repository

This repository holds the code to generate reports based on Riverscapes Consortium data, largely maintained in AWS Athena.

It consists of a python project for each report generator, as well as scripts necessary to run the project with Fargate.

Code to maintain the web UI that allows users to trigger report generation with custom parameters is in another repository, [`rs-reports-monorepo`](https://github.com/Riverscapes/rs-reports-monorepo).

## Setting up & running your own instance

Use `uv sync`.

You may need to [install WeasyPrint following these instructions](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#installation).
